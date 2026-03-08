#!/usr/bin/env python3
"""
Telemetry Monitor Service for MeshCore Bot
Polls repeaters for telemetry data (battery, temperature, etc.) and
accepts MQTT requests for on-demand telemetry queries with path support.
"""

import asyncio
import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import aiohttp

from .base_service import BaseServicePlugin

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

logger = logging.getLogger('TelemetryMonitor')


class TelemetryMonitorService(BaseServicePlugin):
    """Telemetry monitoring service.

    Periodically polls configured repeaters for LPP telemetry data,
    stores readings in a SQLite database, and accepts MQTT requests
    for on-demand telemetry queries with path support (flood/direct/custom).
    """

    config_section = 'TelemetryMonitor'
    description = "Polls repeaters for battery/telemetry data via MeshCore"

    def __init__(self, bot: Any):
        super().__init__(bot)
        self.logger = logging.getLogger('TelemetryMonitor')
        self.logger.setLevel(bot.logger.level)

        config = bot.config

        # Polling settings
        self.poll_interval_minutes: int = config.getint(
            'TelemetryMonitor', 'poll_interval_minutes', fallback=30)
        self.request_timeout: int = config.getint(
            'TelemetryMonitor', 'request_timeout', fallback=60)
        self.retry_delay_seconds: int = config.getint(
            'TelemetryMonitor', 'retry_delay_seconds', fallback=30)
        self.max_retries: int = config.getint(
            'TelemetryMonitor', 'max_retries', fallback=3)
        self.delay_first_poll: bool = config.getboolean(
            'TelemetryMonitor', 'delay_first_poll', fallback=True)
        self.default_path_mode: str = config.get(
            'TelemetryMonitor', 'default_path_mode', fallback='flood')
        self.verbose: bool = config.getboolean(
            'TelemetryMonitor', 'verbose', fallback=False)

        # Database
        self.database_path: str = config.get(
            'TelemetryMonitor', 'database_path', fallback='telemetry_data.db')
        if not os.path.isabs(self.database_path):
            bot_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.database_path = os.path.join(bot_root, self.database_path)

        # MQTT settings – broker connection from shared [MQTT] section
        self.mqtt_enabled: bool = config.getboolean(
            'TelemetryMonitor', 'mqtt_enabled', fallback=False)
        self.mqtt_server: str = config.get(
            'MQTT', 'server', fallback='localhost')
        self.mqtt_port: int = config.getint(
            'MQTT', 'port', fallback=1883)
        self.mqtt_topic_request: str = config.get(
            'TelemetryMonitor', 'mqtt_topic_request', fallback='meshcore/telemetry/request')
        self.mqtt_topic_response: str = config.get(
            'TelemetryMonitor', 'mqtt_topic_response', fallback='meshcore/telemetry/response')
        self.mqtt_transport: str = config.get(
            'MQTT', 'transport', fallback='tcp').lower()

        # Repeaters (initial list from config, DB takes precedence once initialized)
        repeaters_str = config.get('TelemetryMonitor', 'repeaters', fallback='')
        self.repeaters: List[str] = [
            r.strip() for r in repeaters_str.split(',') if r.strip()
        ]

        # Webhook settings (defaults – overridden by DB values from web UI)
        self.webhook_url: str = config.get(
            'TelemetryMonitor', 'webhook_url', fallback='')
        self.webhook_enabled: bool = config.getboolean(
            'TelemetryMonitor', 'webhook_enabled', fallback=False)
        self.webhook_interval_minutes: int = config.getint(
            'TelemetryMonitor', 'webhook_interval_minutes', fallback=0)
        self.webhook_on_threshold: bool = config.getboolean(
            'TelemetryMonitor', 'webhook_on_threshold', fallback=True)
        self.webhook_battery_threshold: float = config.getfloat(
            'TelemetryMonitor', 'webhook_battery_threshold', fallback=20.0)

        # Runtime state
        self._db_conn: Optional[sqlite3.Connection] = None
        self._poll_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._last_poll_time: Optional[datetime] = None
        self._last_webhook_time: Optional[datetime] = None
        self._mqtt_client: Optional[Any] = None
        self._mqtt_connected = False
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

    # ──────────────────────────────────────────────────────────────────
    # Database
    # ──────────────────────────────────────────────────────────────────

    def _init_database(self) -> None:
        """Initialize the telemetry SQLite database."""
        self._db_conn = sqlite3.connect(self.database_path, timeout=10.0)
        self._db_conn.row_factory = sqlite3.Row
        self._db_conn.execute('PRAGMA journal_mode=WAL')

        cursor = self._db_conn.cursor()

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS telemetry_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repeater_name TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                temperature REAL,
                humidity REAL,
                pressure REAL,
                battery_voltage REAL,
                battery_percent REAL,
                latitude REAL,
                longitude REAL,
                altitude REAL,
                raw_data TEXT,
                request_duration_ms INTEGER,
                success INTEGER DEFAULT 1
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS poll_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repeater_name TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                success INTEGER NOT NULL,
                error_message TEXT,
                duration_ms INTEGER DEFAULT 0,
                attempt_number INTEGER DEFAULT 1,
                max_attempts INTEGER DEFAULT 3,
                is_manual INTEGER DEFAULT 0,
                trigger_type TEXT DEFAULT 'interval',
                path_mode TEXT DEFAULT 'flood'
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS adhoc_poll_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                repeater_name TEXT NOT NULL,
                requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'pending',
                result TEXT,
                completed_at TIMESTAMP,
                path_mode TEXT DEFAULT 'flood',
                source TEXT DEFAULT 'webui'
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS monitored_repeaters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                enabled INTEGER DEFAULT 1,
                public_key TEXT,
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sort_order INTEGER DEFAULT 0
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS service_status (
                id INTEGER PRIMARY KEY,
                status TEXT DEFAULT 'stopped',
                current_repeater TEXT,
                current_attempt INTEGER,
                max_attempts INTEGER DEFAULT 3,
                last_update TEXT,
                next_poll_time TEXT,
                poll_cycle_start TEXT,
                repeaters_completed INTEGER DEFAULT 0,
                repeaters_total INTEGER DEFAULT 0,
                poll_interval_minutes INTEGER DEFAULT 30,
                mqtt_enabled TEXT DEFAULT '',
                mqtt_topic_request TEXT DEFAULT '',
                mqtt_topic_response TEXT DEFAULT ''
            )
        ''')

        # Migrate: add columns if missing (existing DBs)
        for col, default in [
            ('poll_interval_minutes', 'INTEGER DEFAULT 30'),
            ('mqtt_enabled', "TEXT DEFAULT ''"),
            ('mqtt_topic_request', "TEXT DEFAULT ''"),
            ('mqtt_topic_response', "TEXT DEFAULT ''"),
        ]:
            try:
                cursor.execute(f'SELECT {col} FROM service_status LIMIT 1')
            except sqlite3.OperationalError:
                cursor.execute(f'ALTER TABLE service_status ADD COLUMN {col} {default}')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_config (
                id INTEGER PRIMARY KEY,
                enabled INTEGER DEFAULT 0,
                url TEXT DEFAULT '',
                interval_minutes INTEGER DEFAULT 0,
                on_threshold INTEGER DEFAULT 1,
                battery_threshold REAL DEFAULT 20.0,
                last_sent TEXT,
                last_error TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS webhook_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                url TEXT,
                status_code INTEGER,
                success INTEGER NOT NULL,
                trigger TEXT DEFAULT 'interval',
                error_message TEXT,
                payload_summary TEXT
            )
        ''')

        # Seed status row
        cursor.execute('''
            INSERT OR IGNORE INTO service_status (id, status, poll_interval_minutes) VALUES (1, 'stopped', ?)
        ''', (self.poll_interval_minutes,))

        # Seed webhook config row (merge initial config values)
        cursor.execute('SELECT COUNT(*) FROM webhook_config')
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO webhook_config
                    (id, enabled, url, interval_minutes, on_threshold, battery_threshold)
                VALUES (1, ?, ?, ?, ?, ?)
            ''', (int(self.webhook_enabled), self.webhook_url,
                  self.webhook_interval_minutes, int(self.webhook_on_threshold),
                  self.webhook_battery_threshold))

        self._db_conn.commit()

        # Load webhook config from DB (web UI overrides)
        self._load_webhook_config()

        # Migrate repeaters from config into DB on first run
        self._migrate_repeaters_from_config()

    def _migrate_repeaters_from_config(self) -> None:
        """Migrate repeaters from config into DB if DB is empty."""
        if not self._db_conn or not self.repeaters:
            return
        cursor = self._db_conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM monitored_repeaters')
        if cursor.fetchone()[0] > 0:
            return  # DB already has repeaters
        for idx, name in enumerate(self.repeaters):
            cursor.execute(
                'INSERT OR IGNORE INTO monitored_repeaters (name, sort_order) VALUES (?, ?)',
                (name, idx))
        self._db_conn.commit()
        logger.info(f"Migrated {len(self.repeaters)} repeaters from config to DB")

    def _load_repeaters_from_db(self) -> None:
        """Reload repeater list from DB."""
        if not self._db_conn:
            return
        cursor = self._db_conn.cursor()
        cursor.execute(
            'SELECT name FROM monitored_repeaters WHERE enabled = 1 ORDER BY sort_order, id')
        self.repeaters = [row['name'] for row in cursor.fetchall()]

    def _load_webhook_config(self) -> None:
        """Load webhook configuration from DB."""
        if not self._db_conn:
            return
        try:
            cursor = self._db_conn.cursor()
            cursor.execute('SELECT * FROM webhook_config WHERE id = 1')
            row = cursor.fetchone()
            if row:
                self.webhook_enabled = bool(row['enabled'])
                self.webhook_url = row['url'] or ''
                self.webhook_interval_minutes = row['interval_minutes'] or 0
                self.webhook_on_threshold = bool(row['on_threshold'])
                self.webhook_battery_threshold = row['battery_threshold'] or 20.0
                if row['last_sent']:
                    try:
                        self._last_webhook_time = datetime.strptime(row['last_sent'], '%Y-%m-%d %H:%M:%S')
                    except (ValueError, TypeError):
                        pass
        except Exception as e:
            logger.debug(f"Webhook config load error: {e}")

    def _update_status(self, status: str, **kwargs: Any) -> None:
        """Update service status in database for web UI polling."""
        if not self._db_conn:
            return
        try:
            fields = {'status': status, 'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            fields.update(kwargs)
            sets = ', '.join(f'{k} = ?' for k in fields)
            vals = list(fields.values())
            self._db_conn.execute(
                f'UPDATE service_status SET {sets} WHERE id = 1', vals)
            self._db_conn.commit()
        except Exception as e:
            logger.debug(f"Status update error: {e}")

    def _reload_poll_interval(self) -> None:
        """Re-read poll_interval_minutes and MQTT topics from DB (set via web UI)."""
        if not self._db_conn:
            return
        try:
            row = self._db_conn.execute(
                'SELECT poll_interval_minutes, mqtt_enabled, mqtt_topic_request, mqtt_topic_response '
                'FROM service_status WHERE id = 1').fetchone()
            if row:
                if row['poll_interval_minutes']:
                    new_val = int(row['poll_interval_minutes'])
                    if new_val >= 1 and new_val != self.poll_interval_minutes:
                        logger.info(f"Poll interval changed: {self.poll_interval_minutes} → {new_val} min")
                        self.poll_interval_minutes = new_val
                if row['mqtt_enabled'] is not None and row['mqtt_enabled'] != '':
                    new_mqtt = row['mqtt_enabled'] == '1'
                    if new_mqtt != self.mqtt_enabled:
                        logger.info(f"MQTT enabled changed: {self.mqtt_enabled} → {new_mqtt}")
                        self.mqtt_enabled = new_mqtt
                if row['mqtt_topic_request']:
                    new_topic = row['mqtt_topic_request']
                    if new_topic != self.mqtt_topic_request:
                        logger.info(f"MQTT request topic changed: {self.mqtt_topic_request} → {new_topic}")
                        self.mqtt_topic_request = new_topic
                if row['mqtt_topic_response']:
                    new_topic = row['mqtt_topic_response']
                    if new_topic != self.mqtt_topic_response:
                        logger.info(f"MQTT response topic changed: {self.mqtt_topic_response} → {new_topic}")
                        self.mqtt_topic_response = new_topic
        except Exception as e:
            logger.debug(f"Poll interval / MQTT reload error: {e}")

    # ──────────────────────────────────────────────────────────────────
    # MQTT
    # ──────────────────────────────────────────────────────────────────

    def _setup_mqtt(self) -> None:
        """Set up and connect the MQTT client for telemetry requests."""
        if not self.mqtt_enabled or mqtt is None:
            if self.mqtt_enabled and mqtt is None:
                logger.warning("MQTT enabled but paho-mqtt not installed")
            return

        try:
            client_id = f"meshcore-telemetry-{os.getpid()}"
            self._mqtt_client = mqtt.Client(
                client_id=client_id,
                transport=self.mqtt_transport)
            self._mqtt_client.on_connect = self._on_mqtt_connect
            self._mqtt_client.on_message = self._on_mqtt_message
            self._mqtt_client.on_disconnect = self._on_mqtt_disconnect

            logger.info(f"MQTT connecting to {self.mqtt_server}:{self.mqtt_port} ({self.mqtt_transport})")
            self._mqtt_client.connect_async(self.mqtt_server, self.mqtt_port, keepalive=60)
            self._mqtt_client.loop_start()
        except Exception as e:
            logger.error(f"MQTT setup error: {e}")

    def _on_mqtt_connect(self, client: Any, userdata: Any, flags: Any, rc: int) -> None:
        if rc == 0:
            self._mqtt_connected = True
            logger.info(f"MQTT connected – subscribing to {self.mqtt_topic_request}")
            client.subscribe(self.mqtt_topic_request, qos=1)
        else:
            logger.warning(f"MQTT connection failed: rc={rc}")

    def _on_mqtt_disconnect(self, client: Any, userdata: Any, rc: int) -> None:
        self._mqtt_connected = False
        if rc != 0:
            logger.warning(f"MQTT disconnected unexpectedly: rc={rc}")

    def _on_mqtt_message(self, client: Any, userdata: Any, msg: Any) -> None:
        """Handle incoming MQTT telemetry request.

        Expected JSON:
            {"repeater": "RepeaterName", "path": "flood"}

        Supported path values:
            - "flood"   – reset path to use flood routing (default)
            - "direct"  – use existing known path
            - "ef,10"   – custom hex path bytes
        """
        try:
            payload_str = msg.payload.decode('utf-8', errors='replace')
            logger.info(f"MQTT request received: {payload_str[:200]}")

            data = json.loads(payload_str)
            repeater_name = data.get('repeater', '').strip()
            path_mode = data.get('path', self.default_path_mode).strip().lower()

            if not repeater_name:
                self._publish_mqtt_response({
                    'success': False, 'error': 'Missing "repeater" field'})
                return

            logger.info(f"MQTT ad-hoc request: {repeater_name} (path={path_mode})")

            # Schedule async execution (DB insert happens in event loop thread)
            if self._event_loop and not self._event_loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self._handle_mqtt_request(repeater_name, path_mode),
                    self._event_loop)
        except json.JSONDecodeError:
            logger.warning(f"MQTT: invalid JSON on {msg.topic}")
            self._publish_mqtt_response({
                'success': False, 'error': 'Invalid JSON payload'})
        except Exception as e:
            logger.error(f"MQTT message handler error: {e}")

    async def _handle_mqtt_request(self, repeater_name: str, path_mode: str) -> None:
        """Process an MQTT-triggered telemetry request."""
        try:
            # Insert ad-hoc request into DB (must run in event loop thread)
            if self._db_conn:
                cursor = self._db_conn.cursor()
                cursor.execute('''
                    INSERT INTO adhoc_poll_requests
                        (repeater_name, status, path_mode, source)
                    VALUES (?, 'pending', ?, 'mqtt')
                ''', (repeater_name, path_mode))
                self._db_conn.commit()

            contact = await self._find_contact(repeater_name)
            if not contact:
                self._publish_mqtt_response({
                    'repeater': repeater_name,
                    'success': False,
                    'error': f'Contact "{repeater_name}" not found'
                })
                return

            max_attempts = self.max_retries
            for attempt in range(1, max_attempts + 1):
                t0 = time.time()
                telemetry_data = await self._request_telemetry(contact, path_mode)
                duration_ms = int((time.time() - t0) * 1000)

                if telemetry_data:
                    parsed = self._parse_lpp_data(telemetry_data)
                    self._store_reading(repeater_name, parsed, telemetry_data, duration_ms)
                    response = {
                        'repeater': repeater_name,
                        'success': True,
                        'path': path_mode,
                        'attempt': f'{attempt}/{max_attempts}',
                        'duration_ms': duration_ms,
                        **parsed,
                    }
                    self._publish_mqtt_response(response)
                    return

                # Publish intermediate failure so subscriber sees progress
                self._publish_mqtt_response({
                    'repeater': repeater_name,
                    'success': False,
                    'path': path_mode,
                    'attempt': f'{attempt}/{max_attempts}',
                    'duration_ms': duration_ms,
                    'error': 'No telemetry response (timeout)',
                })
                if attempt < max_attempts:
                    logger.info(f"MQTT request retry {attempt}/{max_attempts} for {repeater_name}")
        except Exception as e:
            logger.error(f"MQTT request processing error: {e}")
            self._publish_mqtt_response({
                'repeater': repeater_name,
                'success': False,
                'error': str(e)
            })

    def _publish_mqtt_response(self, data: Dict[str, Any]) -> None:
        """Publish a telemetry response to the MQTT response topic."""
        if not self._mqtt_client or not self._mqtt_connected:
            return
        try:
            payload = json.dumps(data, default=str)
            self._mqtt_client.publish(self.mqtt_topic_response, payload, qos=1)
            logger.debug(f"MQTT response published: {payload[:200]}")
        except Exception as e:
            logger.error(f"MQTT publish error: {e}")

    def _publish_telemetry_reading(self, repeater_name: str, parsed: Dict[str, Any],
                                   raw_data: List[Dict], duration_ms: int) -> None:
        """Publish a telemetry reading to MQTT after successful poll."""
        if not self._mqtt_client or not self._mqtt_connected:
            return
        try:
            payload = json.dumps({
                'repeater': repeater_name,
                'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                'duration_ms': duration_ms,
                **parsed,
                'raw_data': raw_data,
            }, default=str)
            topic = self.mqtt_topic_response
            self._mqtt_client.publish(topic, payload, qos=1, retain=True)
            logger.info(f"MQTT telemetry published: {topic}")
        except Exception as e:
            logger.error(f"MQTT telemetry publish error: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Service lifecycle
    # ──────────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the telemetry monitoring service."""
        logger.info("Starting TelemetryMonitor service...")
        self._running = True
        self._stop_event.clear()
        self._event_loop = asyncio.get_event_loop()

        self._init_database()
        self._load_repeaters_from_db()
        self._update_status('starting')

        # MQTT
        self._setup_mqtt()

        # Start polling loop
        self._poll_task = asyncio.create_task(self._polling_loop())

        logger.info(
            f"TelemetryMonitor started – {len(self.repeaters)} repeaters, "
            f"interval={self.poll_interval_minutes}min, "
            f"MQTT={'connected' if self._mqtt_connected else ('enabled' if self.mqtt_enabled else 'disabled')}")

    async def stop(self) -> None:
        """Stop the telemetry monitoring service."""
        logger.info("Stopping TelemetryMonitor service...")
        self._running = False
        self._stop_event.set()
        self._update_status('stopping')

        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None

        # MQTT cleanup
        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception:
                pass
            self._mqtt_client = None

        if self._db_conn:
            self._update_status('stopped')
            self._db_conn.close()
            self._db_conn = None

        logger.info("TelemetryMonitor stopped")

    # ──────────────────────────────────────────────────────────────────
    # Polling loop
    # ──────────────────────────────────────────────────────────────────

    async def _polling_loop(self) -> None:
        """Main polling loop – periodic + ad-hoc request processing."""
        initial_delay = (self.poll_interval_minutes * 60) if self.delay_first_poll else 10
        next_poll_time = datetime.now() + timedelta(seconds=initial_delay)

        self._update_status('waiting', next_poll_time=next_poll_time.strftime('%Y-%m-%d %H:%M:%S'),
                            repeaters_total=len(self.repeaters))

        # Wait for initial delay, checking for ad-hoc requests every 5s
        remaining = initial_delay
        while remaining > 0 and not self._stop_event.is_set():
            await self._process_adhoc_requests()
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=min(5, remaining))
                return
            except asyncio.TimeoutError:
                remaining -= 5

        while not self._stop_event.is_set():
            try:
                await self._poll_all_repeaters()
            except Exception as e:
                logger.error(f"Polling loop error: {e}", exc_info=True)

            self._last_poll_time = datetime.now()
            self._reload_poll_interval()
            next_poll = datetime.now() + timedelta(minutes=self.poll_interval_minutes)
            self._update_status('waiting', current_repeater=None, current_attempt=None,
                                next_poll_time=next_poll.strftime('%Y-%m-%d %H:%M:%S'),
                                repeaters_completed=0, repeaters_total=len(self.repeaters))

            wait_seconds = self.poll_interval_minutes * 60
            while wait_seconds > 0 and not self._stop_event.is_set():
                await self._process_adhoc_requests()
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=min(5, wait_seconds))
                    break
                except asyncio.TimeoutError:
                    wait_seconds -= 5

    async def _poll_all_repeaters(self) -> None:
        """Poll all configured repeaters."""
        self._load_repeaters_from_db()
        if not self.repeaters:
            logger.warning("No repeaters configured, skipping poll cycle")
            return

        logger.info(f"Starting telemetry poll cycle for {len(self.repeaters)} repeaters")
        self._update_status('polling', poll_cycle_start=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            repeaters_completed=0, repeaters_total=len(self.repeaters))

        for idx, name in enumerate(self.repeaters):
            if self._stop_event.is_set():
                break
            try:
                await self._poll_single_repeater(name, idx)
            except Exception as e:
                logger.error(f"Error polling {name}: {e}")
                self._record_poll_attempt(name, False, str(e), 0)

            self._update_status('polling', current_repeater=None,
                                repeaters_completed=idx + 1, repeaters_total=len(self.repeaters))

            # Wait between repeaters to avoid flooding the network
            if idx < len(self.repeaters) - 1:
                logger.info("Waiting 60s before next repeater...")
                self._update_status('pausing',
                                    current_repeater=f"Warte 60s vor {self.repeaters[idx + 1]}",
                                    repeaters_completed=idx + 1, repeaters_total=len(self.repeaters))
                await asyncio.sleep(60)

    async def _poll_single_repeater(self, repeater_name: str, repeater_idx: int = 0,
                                     is_manual: bool = False,
                                     path_mode: Optional[str] = None) -> bool:
        """Poll a single repeater with retry logic.

        Returns:
            True if telemetry data was received, False otherwise.
        """
        if path_mode is None:
            path_mode = self.default_path_mode
        logger.info(f"Polling telemetry from: {repeater_name} (path={path_mode})"
                     + (" [manual]" if is_manual else ""))

        self._update_status('looking_up', current_repeater=repeater_name, current_attempt=0,
                            repeaters_completed=repeater_idx, repeaters_total=len(self.repeaters))

        contact = await self._find_contact(repeater_name)
        if not contact:
            error_msg = f"Contact '{repeater_name}' not found in radio contact list"
            logger.warning(f"POLL ABORTED: {error_msg}")
            self._record_poll_attempt(repeater_name, False, error_msg, 0,
                                      is_manual=is_manual, path_mode=path_mode)
            return False

        for attempt in range(1, self.max_retries + 1):
            start_time = datetime.now()
            self._update_status('requesting', current_repeater=repeater_name,
                                current_attempt=attempt, max_attempts=self.max_retries,
                                repeaters_completed=repeater_idx, repeaters_total=len(self.repeaters))
            try:
                telemetry_data = await self._request_telemetry(contact, path_mode)
                duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

                if telemetry_data:
                    parsed = self._parse_lpp_data(telemetry_data)
                    self._store_reading(repeater_name, parsed, telemetry_data, duration_ms)
                    self._record_poll_attempt(repeater_name, True, None, duration_ms,
                                              attempt_number=attempt, is_manual=is_manual,
                                              path_mode=path_mode)
                    if self.verbose:
                        logger.info(f"Telemetry from {repeater_name}: {parsed}")
                    return True
                else:
                    logger.warning(
                        f"No telemetry data from {repeater_name} (attempt {attempt}/{self.max_retries})")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Telemetry timeout for {repeater_name} (attempt {attempt}/{self.max_retries})")
            except Exception as e:
                logger.error(
                    f"Telemetry error for {repeater_name}: {e} (attempt {attempt}/{self.max_retries})")

            if attempt < self.max_retries:
                self._update_status('retry_wait', current_repeater=repeater_name,
                                    current_attempt=attempt, max_attempts=self.max_retries)
                await asyncio.sleep(self.retry_delay_seconds)

        # All retries exhausted
        error_msg = f"No response after {self.max_retries} attempts (path={path_mode})"
        self._record_poll_attempt(repeater_name, False, error_msg, 0,
                                  attempt_number=self.max_retries, is_manual=is_manual,
                                  path_mode=path_mode)
        logger.warning(f"POLL FAILED: {repeater_name} – {error_msg}")
        return False

    # ──────────────────────────────────────────────────────────────────
    # Ad-hoc requests (from web UI and MQTT)
    # ──────────────────────────────────────────────────────────────────

    async def _process_adhoc_requests(self) -> None:
        """Process pending ad-hoc poll requests from the database."""
        if not self._db_conn:
            return
        try:
            cursor = self._db_conn.cursor()
            cursor.execute('''
                SELECT id, repeater_name, path_mode, source
                FROM adhoc_poll_requests
                WHERE status = 'pending'
                ORDER BY requested_at ASC LIMIT 1
            ''')
            row = cursor.fetchone()
            if not row:
                return

            req_id = row['id']
            repeater_name = row['repeater_name']
            path_mode = row['path_mode'] or self.default_path_mode
            source = row['source'] or 'webui'

            logger.info(f"Processing ad-hoc request #{req_id} for {repeater_name} (path={path_mode}, source={source})")

            cursor.execute("UPDATE adhoc_poll_requests SET status = 'processing' WHERE id = ?", (req_id,))
            self._db_conn.commit()

            try:
                self._update_status('adhoc_polling', current_repeater=repeater_name)
                success = await self._poll_single_repeater(repeater_name, is_manual=True, path_mode=path_mode)

                if success:
                    cursor.execute('''
                        UPDATE adhoc_poll_requests
                        SET status = 'completed', result = 'success', completed_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (req_id,))
                else:
                    cursor.execute('''
                        UPDATE adhoc_poll_requests
                        SET status = 'failed', result = 'no_response', completed_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    ''', (req_id,))
                self._db_conn.commit()
            except Exception as e:
                cursor.execute('''
                    UPDATE adhoc_poll_requests
                    SET status = 'failed', result = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (str(e), req_id))
                self._db_conn.commit()
                logger.error(f"Ad-hoc request #{req_id} failed: {e}")

            self._update_status('waiting', current_repeater=None, current_attempt=None)
        except Exception as e:
            logger.error(f"Error processing ad-hoc requests: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Contact lookup & telemetry request
    # ──────────────────────────────────────────────────────────────────

    async def _find_contact(self, repeater_name: str) -> Optional[Any]:
        """Find a contact by name in the bot's contact list."""
        if not hasattr(self.bot, 'meshcore') or not self.bot.meshcore:
            logger.error("Bot meshcore client not available")
            return None
        try:
            # Exact match
            contact = self.bot.meshcore.get_contact_by_name(repeater_name)
            if contact:
                return contact

            # Partial match
            contacts = self.bot.meshcore.contacts if hasattr(self.bot.meshcore, 'contacts') else []
            for c in contacts:
                c_name = c.get('adv_name', '') or c.get('name', '') if isinstance(c, dict) else (
                    getattr(c, 'adv_name', '') or getattr(c, 'name', ''))
                if repeater_name.lower() in c_name.lower():
                    logger.info(f"Found partial match: {c_name} for {repeater_name}")
                    return c

            # Fallback: stored public key in monitored_repeaters
            if self._db_conn:
                cursor = self._db_conn.cursor()
                cursor.execute('SELECT public_key FROM monitored_repeaters WHERE name = ?', (repeater_name,))
                row = cursor.fetchone()
                if row and row['public_key']:
                    contact = self.bot.meshcore.get_contact_by_key_prefix(row['public_key'][:12])
                    if contact:
                        logger.info(f"Found contact for {repeater_name} via stored public key")
                        return contact

            logger.warning(f"Contact not found: '{repeater_name}'")
            return None
        except Exception as e:
            logger.error(f"Error finding contact {repeater_name}: {e}")
            return None

    async def _request_telemetry(self, contact: Any, path_mode: str = 'flood') -> Optional[List[Dict]]:
        """Request telemetry data from a contact.

        Args:
            contact: The meshcore contact object.
            path_mode: Routing mode – 'flood', 'direct', or comma-separated hex
                       bytes (e.g. 'ef,10') for a custom path.
        """
        if not hasattr(self.bot, 'meshcore') or not self.bot.meshcore:
            return None

        try:
            from meshcore import EventType

            contact_key = (contact.get('public_key') if isinstance(contact, dict)
                           else getattr(contact, 'public_key', None))

            if path_mode == 'flood' and contact_key:
                # Reset path to force flood routing
                logger.info(f"Resetting path for flood routing to {contact_key[:12]}...")
                try:
                    reset_result = await self.bot.meshcore.commands.reset_path(contact_key)
                    if reset_result.type == EventType.ERROR:
                        logger.warning(f"Could not reset path: {reset_result.payload}")
                    else:
                        logger.info("Path reset – using flood routing")
                except Exception as e:
                    logger.warning(f"Path reset error: {e}, continuing...")
            elif path_mode == 'direct':
                logger.info("Using direct (existing) path")
            elif path_mode and path_mode not in ('flood', 'direct'):
                # Custom hex path, e.g. "ef,10"
                logger.info(f"Custom path requested: {path_mode}")
                try:
                    path_bytes = bytes(int(b, 16) for b in path_mode.split(','))
                    if contact_key:
                        await self.bot.meshcore.commands.set_path(contact_key, path_bytes)
                        logger.info(f"Custom path set: {path_bytes.hex()}")
                except Exception as e:
                    logger.warning(f"Could not set custom path '{path_mode}': {e}, using flood")
                    if contact_key:
                        try:
                            await self.bot.meshcore.commands.reset_path(contact_key)
                        except Exception:
                            pass

            result = await self.bot.meshcore.commands.req_telemetry_sync(
                contact, timeout=self.request_timeout)

            if result is not None and len(result) > 0:
                logger.info(f"Received telemetry: {len(result)} sensor values")
                return result

            logger.warning(f"No telemetry response (timeout={self.request_timeout}s)")
            return None
        except Exception as e:
            logger.error(f"Telemetry request error: {e}")
            raise

    # ──────────────────────────────────────────────────────────────────
    # LPP parsing & storage
    # ──────────────────────────────────────────────────────────────────

    def _parse_lpp_data(self, data: List[Dict]) -> Dict[str, Any]:
        """Parse already-decoded LPP telemetry data from meshcore."""
        result: Dict[str, Any] = {
            'temperature': None,
            'humidity': None,
            'pressure': None,
            'battery_voltage': None,
            'battery_percent': None,
            'latitude': None,
            'longitude': None,
            'altitude': None,
        }
        if not data:
            return result

        for item in data:
            data_type = item.get('type', '')
            value = item.get('value')

            if data_type == 'temperature' and value is not None:
                result['temperature'] = float(value)
            elif data_type == 'humidity' and value is not None:
                result['humidity'] = float(value)
            elif data_type in ('barometer', 'pressure') and value is not None:
                result['pressure'] = float(value)
            elif data_type == 'altitude' and value is not None:
                result['altitude'] = float(value)
            elif data_type == 'voltage' and value is not None:
                result['battery_voltage'] = float(value)
                voltage = float(value)
                percent = (voltage - 3.0) / (4.2 - 3.0) * 100
                result['battery_percent'] = max(0.0, min(100.0, percent))
            elif data_type == 'gps' and isinstance(value, dict):
                result['latitude'] = value.get('lat')
                result['longitude'] = value.get('lon')
                if result['altitude'] is None:
                    result['altitude'] = value.get('alt')

        return result

    def _store_reading(self, repeater_name: str, parsed: Dict[str, Any],
                       raw_data: List[Dict], duration_ms: int) -> None:
        """Store a telemetry reading."""
        if not self._db_conn:
            return
        try:
            raw_json = json.dumps(raw_data) if raw_data else None
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._db_conn.execute('''
                INSERT INTO telemetry_readings
                    (repeater_name, timestamp, temperature, humidity, pressure,
                     battery_voltage, battery_percent, latitude, longitude, altitude,
                     raw_data, request_duration_ms, success)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (repeater_name, ts,
                  parsed.get('temperature'), parsed.get('humidity'), parsed.get('pressure'),
                  parsed.get('battery_voltage'), parsed.get('battery_percent'),
                  parsed.get('latitude'), parsed.get('longitude'), parsed.get('altitude'),
                  raw_json, duration_ms, True))
            self._db_conn.commit()
            logger.info(f"Stored telemetry for {repeater_name}: "
                        f"voltage={parsed.get('battery_voltage')}, "
                        f"temp={parsed.get('temperature')}")

            # Publish to MQTT
            self._publish_telemetry_reading(repeater_name, parsed, raw_data, duration_ms)

            # Webhook: check threshold + interval
            self._maybe_send_webhook(repeater_name, parsed)
        except Exception as e:
            logger.error(f"Error storing reading: {e}")

    def _record_poll_attempt(self, repeater_name: str, success: bool,
                             error_message: Optional[str], duration_ms: int,
                             attempt_number: int = 1,
                             is_manual: bool = False,
                             path_mode: str = 'flood') -> None:
        """Record a poll attempt for debugging."""
        if not self._db_conn:
            return
        try:
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._db_conn.execute('''
                INSERT INTO poll_attempts
                    (repeater_name, timestamp, success, error_message, duration_ms,
                     attempt_number, max_attempts, is_manual, trigger_type, path_mode)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (repeater_name, ts, success, error_message, duration_ms,
                  attempt_number, self.max_retries, is_manual,
                  'manual' if is_manual else 'interval', path_mode))
            self._db_conn.commit()
        except Exception as e:
            logger.error(f"Error recording poll attempt: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Webhook
    # ──────────────────────────────────────────────────────────────────

    def _maybe_send_webhook(self, repeater_name: str, parsed: Dict[str, Any]) -> None:
        """Decide whether to fire the webhook after a successful reading."""
        self._load_webhook_config()  # pick up web UI changes
        if not self.webhook_enabled or not self.webhook_url:
            return

        trigger = None

        # Threshold alert
        if self.webhook_on_threshold:
            bp = parsed.get('battery_percent')
            if bp is not None and bp < self.webhook_battery_threshold:
                trigger = 'threshold'
                logger.info(
                    f"Webhook threshold triggered: {repeater_name} battery {bp:.1f}% "
                    f"< {self.webhook_battery_threshold}%")

        # Interval-based
        if trigger is None and self.webhook_interval_minutes > 0:
            if self._last_webhook_time is None:
                trigger = 'interval'
            else:
                elapsed = (datetime.now() - self._last_webhook_time).total_seconds() / 60
                if elapsed >= self.webhook_interval_minutes:
                    trigger = 'interval'

        if trigger is None:
            return

        # Gather all latest readings for the payload
        readings = self.get_latest_readings()
        payload = {
            'trigger': trigger,
            'triggered_by': repeater_name,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'readings': readings,
        }
        if trigger == 'threshold':
            payload['threshold'] = self.webhook_battery_threshold

        if self._event_loop and not self._event_loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                self._send_webhook(payload, trigger), self._event_loop)

    async def _send_webhook(self, payload: Dict[str, Any], trigger: str) -> None:
        """POST the payload to the configured webhook URL."""
        url = self.webhook_url
        try:
            body = json.dumps(payload, default=str)
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    url,
                    data=body,
                    headers={'Content-Type': 'application/json'},
                ) as resp:
                    status_code = resp.status
                    success = 200 <= status_code < 300
                    if success:
                        logger.info(f"Webhook sent ({trigger}): {status_code}")
                        self._last_webhook_time = datetime.now()
                        self._save_webhook_last_sent()
                    else:
                        resp_text = await resp.text()
                        logger.warning(f"Webhook failed: {status_code} – {resp_text[:200]}")
                    self._log_webhook(url, status_code, success, trigger)
        except Exception as e:
            logger.error(f"Webhook error: {e}")
            self._log_webhook(url, 0, False, trigger, str(e))

    def _save_webhook_last_sent(self) -> None:
        if not self._db_conn:
            return
        try:
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._db_conn.execute(
                'UPDATE webhook_config SET last_sent = ? WHERE id = 1', (ts,))
            self._db_conn.commit()
        except Exception:
            pass

    def _log_webhook(self, url: str, status_code: int, success: bool,
                     trigger: str, error: Optional[str] = None) -> None:
        if not self._db_conn:
            return
        try:
            ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self._db_conn.execute('''
                INSERT INTO webhook_log
                    (timestamp, url, status_code, success, trigger, error_message)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (ts, url, status_code, int(success), trigger, error))
            if not success and error:
                self._db_conn.execute(
                    'UPDATE webhook_config SET last_error = ? WHERE id = 1', (error,))
            self._db_conn.commit()
        except Exception as e:
            logger.debug(f"Webhook log error: {e}")

    # ──────────────────────────────────────────────────────────────────
    # Public API for web viewer
    # ──────────────────────────────────────────────────────────────────

    def get_latest_readings(self, repeater_name: Optional[str] = None) -> List[Dict]:
        """Get latest telemetry readings."""
        if not self._db_conn:
            return []
        try:
            cursor = self._db_conn.cursor()
            if repeater_name:
                cursor.execute('''
                    SELECT * FROM telemetry_readings
                    WHERE repeater_name = ? AND success = 1
                    ORDER BY timestamp DESC LIMIT 1
                ''', (repeater_name,))
            else:
                cursor.execute('''
                    SELECT t1.* FROM telemetry_readings t1
                    INNER JOIN (
                        SELECT repeater_name, MAX(timestamp) as max_ts
                        FROM telemetry_readings WHERE success = 1
                        GROUP BY repeater_name
                    ) t2 ON t1.repeater_name = t2.repeater_name AND t1.timestamp = t2.max_ts
                    ORDER BY t1.repeater_name
                ''')
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting readings: {e}")
            return []

    def get_poll_history(self, limit: int = 50) -> List[Dict]:
        """Get recent poll attempts."""
        if not self._db_conn:
            return []
        try:
            cursor = self._db_conn.cursor()
            cursor.execute('''
                SELECT * FROM poll_attempts
                ORDER BY timestamp DESC LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting poll history: {e}")
            return []
