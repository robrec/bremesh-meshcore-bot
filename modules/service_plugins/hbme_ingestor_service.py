#!/usr/bin/env python3
"""
HBME Ingestor Service
Sends packet data to the HBME API (api.hbme.sh) for network analysis and visualization.
Authentication via Authelia SSO (username/password → session cookie).
"""

import asyncio
import aiohttp
import json
import logging
import time
import copy
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import deque
from http.cookies import SimpleCookie

from meshcore import EventType

from ..enums import PayloadType, RouteType, AdvertFlags
from ..utils import calculate_packet_hash
from .base_service import BaseServicePlugin


class HBMEIngestorService(BaseServicePlugin):
    """Service that sends packet data to the HBME API.
    
    Captures packets from MeshCore network and forwards them to
    api.hbme.sh for network analysis and visualization.
    """
    
    config_section = 'HBMEIngestor'
    description = "Sends packet data to HBME API for network analysis (Authelia SSO)"
    
    # Default Authelia first-factor endpoint (relative to API base URL)
    DEFAULT_AUTH_URL = 'https://auth.hbme.sh/api/firstfactor'
    
    # Route type mapping (internal -> API format)
    ROUTE_TYPE_MAP = {
        'TRANSPORT_FLOOD': 'ROUTE_TYPE_TRANSPORT_FLOOD',
        'FLOOD': 'ROUTE_TYPE_FLOOD',
        'DIRECT': 'ROUTE_TYPE_DIRECT',
        'TRANSPORT_DIRECT': 'ROUTE_TYPE_TRANSPORT_DIRECT',
        'UNKNOWN': 'ROUTE_TYPE_FLOOD'  # Default fallback
    }
    
    # Payload type mapping (internal -> API format)
    PAYLOAD_TYPE_MAP = {
        'REQ': 'PAYLOAD_TYPE_REQ',
        'RESPONSE': 'PAYLOAD_TYPE_RESPONSE',
        'TXT_MSG': 'PAYLOAD_TYPE_TXT_MSG',
        'ACK': 'PAYLOAD_TYPE_ACK',
        'ADVERT': 'PAYLOAD_TYPE_ADVERT',
        'GRP_TXT': 'PAYLOAD_TYPE_GRP_TXT',
        'GRP_DATA': 'PAYLOAD_TYPE_GRP_DATA',
        'ANON_REQ': 'PAYLOAD_TYPE_ANON_REQ',
        'PATH': 'PAYLOAD_TYPE_PATH',
        'TRACE': 'PAYLOAD_TYPE_TRACE',
        'MULTIPART': 'PAYLOAD_TYPE_MULTIPART',
        'RAW_CUSTOM': 'PAYLOAD_TYPE_RAW_CUSTOM',
        'UNKNOWN': 'PAYLOAD_TYPE_REQ'  # Default fallback
    }
    
    def __init__(self, bot):
        """Initialize HBME Ingestor service.
        
        Args:
            bot: The bot instance.
        """
        super().__init__(bot)
        
        # Setup logging
        self.logger = logging.getLogger('HBMEIngestorService')
        self.logger.setLevel(bot.logger.level)
        
        # Only setup handlers if none exist
        if not self.logger.handlers:
            for handler in bot.logger.handlers:
                if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                    self.logger.addHandler(handler)
                    break
        self.logger.propagate = False
        
        # Load configuration from database (web UI) or fallback to config file
        self._load_config()
        
        # Statistics tracking
        self.stats = {
            'packets_sent': 0,
            'packets_failed': 0,
            'last_send_time': None,
            'last_error': None,
            'last_error_time': None,
            'api_response_times': deque(maxlen=100)  # Last 100 response times
        }
        
        # Preview queue - stores last N formatted packets for inspection
        self.preview_queue: deque = deque(maxlen=50)
        
        # Queue for batching (optional future feature)
        self.packet_queue: deque = deque(maxlen=1000)
        
        # HTTP session (reusable)
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Authelia session state
        self._auth_cookie_jar: Optional[aiohttp.CookieJar] = None
        self._authenticated = False
        self._auth_retry_count = 0
        self._max_auth_retries = 3
        self._session_cookie: Optional[str] = None  # authelia_session cookie value
        
        # Event subscriptions
        self.event_subscriptions = []
        
        # Load persisted stats from database
        self._load_stats_from_db()
        
        self.logger.info("HBME Ingestor service initialized")
    
    def _load_config(self) -> None:
        """Load configuration from database (WebUI) with config.ini fallback."""
        config = self.bot.config
        db = self.bot.db_manager
        
        # Check if enabled (database takes precedence)
        db_enabled = db.get_metadata('hbme_ingestor_enabled')
        if db_enabled is not None:
            self.enabled = db_enabled == 'true'
        else:
            self.enabled = config.getboolean('HBMEIngestor', 'enabled', fallback=False)
        
        # API URL (database takes precedence)
        db_url = db.get_metadata('hbme_ingestor_api_url')
        if db_url:
            self.api_url = db_url
        else:
            self.api_url = config.get('HBMEIngestor', 'api_url', 
                                      fallback='https://api.hbme.sh/ingestor/auth/packet')
        
        # Authelia auth URL (database takes precedence)
        db_auth_url = db.get_metadata('hbme_ingestor_auth_url')
        if db_auth_url:
            self.auth_url = db_auth_url
        else:
            self.auth_url = config.get('HBMEIngestor', 'auth_url', 
                                       fallback=self.DEFAULT_AUTH_URL)
        
        # Username (database takes precedence)
        db_username = db.get_metadata('hbme_ingestor_username')
        if db_username:
            self.username = db_username
        else:
            self.username = config.get('HBMEIngestor', 'username', fallback='')
        
        # Password (database takes precedence - stored in DB)
        db_password = db.get_metadata('hbme_ingestor_password')
        if db_password:
            self.password = db_password
        else:
            self.password = config.get('HBMEIngestor', 'password', fallback='')
        
        # Request timeout
        self.timeout = config.getint('HBMEIngestor', 'timeout', fallback=10)
        
        # Retry settings
        self.max_retries = config.getint('HBMEIngestor', 'max_retries', fallback=3)
        self.retry_delay = config.getfloat('HBMEIngestor', 'retry_delay', fallback=1.0)
        
        # Debug mode
        self.debug = config.getboolean('HBMEIngestor', 'debug', fallback=False)
        
        # Preview mode (capture packets but don't send)
        db_preview = db.get_metadata('hbme_ingestor_preview_mode')
        if db_preview is not None:
            self.preview_mode = db_preview == 'true'
        else:
            self.preview_mode = config.getboolean('HBMEIngestor', 'preview_mode', fallback=True)
    
    def reload_config(self) -> None:
        """Reload configuration from database."""
        self._load_config()
        self.logger.info("HBME Ingestor configuration reloaded")
    
    def _load_stats_from_db(self) -> None:
        """Load persisted stats from database."""
        try:
            db = self.bot.db_manager
            
            # Load numeric stats
            sent = db.get_metadata('hbme_stats_packets_sent')
            if sent:
                self.stats['packets_sent'] = int(sent)
            
            failed = db.get_metadata('hbme_stats_packets_failed')
            if failed:
                self.stats['packets_failed'] = int(failed)
            
            # Load timestamps
            self.stats['last_send_time'] = db.get_metadata('hbme_stats_last_send_time')
            self.stats['last_error'] = db.get_metadata('hbme_stats_last_error')
            self.stats['last_error_time'] = db.get_metadata('hbme_stats_last_error_time')
            
        except Exception as e:
            self.logger.warning(f"Could not load stats from DB: {e}")
    
    def _save_stats_to_db(self) -> None:
        """Persist stats to database for WebViewer access."""
        try:
            db = self.bot.db_manager
            
            # Save stats
            db.set_metadata('hbme_stats_packets_sent', str(self.stats['packets_sent']))
            db.set_metadata('hbme_stats_packets_failed', str(self.stats['packets_failed']))
            db.set_metadata('hbme_stats_packets_captured', str(len(self.preview_queue)))
            db.set_metadata('hbme_stats_running', 'true' if self._running else 'false')
            
            # Calculate avg response time
            avg_response_time = 0
            if self.stats['api_response_times']:
                avg_response_time = sum(self.stats['api_response_times']) / len(self.stats['api_response_times'])
            db.set_metadata('hbme_stats_avg_response_ms', str(round(avg_response_time * 1000, 2)))
            
            # Save timestamps
            if self.stats['last_send_time']:
                db.set_metadata('hbme_stats_last_send_time', self.stats['last_send_time'])
            if self.stats['last_error']:
                db.set_metadata('hbme_stats_last_error', self.stats['last_error'])
            if self.stats['last_error_time']:
                db.set_metadata('hbme_stats_last_error_time', self.stats['last_error_time'])
            
            # Save preview queue as JSON for WebViewer
            preview_data = list(self.preview_queue)
            db.set_metadata('hbme_preview_queue', json.dumps(preview_data[-50:]))  # Last 50 packets
            # Note: preview_mode is ONLY written by explicit user action (set_preview_mode)
            # Never overwrite it here to avoid race conditions
            
        except Exception as e:
            self.logger.warning(f"Could not save stats to DB: {e}")
    
    @property
    def meshcore(self):
        """Get meshcore connection from bot."""
        return self.bot.meshcore if self.bot else None
    
    async def start(self) -> None:
        """Start the HBME Ingestor service."""
        if not self.enabled:
            self.logger.info("HBME Ingestor service is disabled")
            return
        
        # In preview mode, we don't need credentials
        if not self.preview_mode and (not self.username or not self.password):
            self.logger.warning("HBME Ingestor: No credentials configured and not in preview mode, service disabled")
            return
        
        # Wait for bot connection
        max_wait = 30
        wait_time = 0
        while (not self.bot.connected or not self.meshcore) and wait_time < max_wait:
            await asyncio.sleep(0.5)
            wait_time += 0.5
        
        if not self.bot.connected or not self.meshcore:
            self.logger.warning("Bot not connected, cannot start HBME Ingestor")
            return
        
        self.logger.info("Starting HBME Ingestor service...")
        
        # Create cookie jar and HTTP session for Authelia SSO
        self._auth_cookie_jar = aiohttp.CookieJar(unsafe=True)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            cookie_jar=self._auth_cookie_jar
        )
        
        # Authenticate with Authelia
        if not self.preview_mode:
            auth_result = await self.authenticate()
            if not auth_result:
                self.logger.warning("HBME Ingestor: Authelia authentication failed, will retry on first packet")
        
        # Setup event handlers
        await self.setup_event_handlers()
        
        self._running = True
        self._save_stats_to_db()  # Update running state in DB
        self.logger.info(f"HBME Ingestor service started (API: {self.api_url})")
    
    async def stop(self) -> None:
        """Stop the HBME Ingestor service."""
        self.logger.info("Stopping HBME Ingestor service...")
        
        self._running = False
        self._authenticated = False
        self._save_stats_to_db()  # Update running state in DB
        
        # Close HTTP session
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._auth_cookie_jar = None
        
        # Clear event subscriptions
        self.event_subscriptions = []
        
        self.logger.info(f"HBME Ingestor service stopped. Stats: sent={self.stats['packets_sent']}, failed={self.stats['packets_failed']}")
    
    async def setup_event_handlers(self) -> None:
        """Setup event handlers for packet capture."""
        if not self.meshcore:
            self.logger.warning("No meshcore connection, cannot setup event handlers")
            return
        
        async def on_rx_log_data(event, metadata=None):
            await self.handle_rx_log_data(event, metadata)
        
        self.meshcore.subscribe(EventType.RX_LOG_DATA, on_rx_log_data)
        self.event_subscriptions = [(EventType.RX_LOG_DATA, on_rx_log_data)]
        
        self.logger.info("HBME Ingestor event handlers registered")
    
    async def handle_rx_log_data(self, event: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Handle RX log data events.
        
        Args:
            event: The RX log data event.
            metadata: Optional metadata dictionary.
        """
        if not self._running or not self.enabled:
            return
        
        try:
            # Copy payload to avoid issues
            payload = copy.deepcopy(event.payload) if hasattr(event, 'payload') else None
            if payload is None:
                self.logger.warning("Event has no payload")
                return
            
            if 'snr' not in payload:
                return
            
            # Get raw hex data
            raw_hex = None
            if 'payload' in payload and payload['payload']:
                raw_hex = payload['payload']
            elif 'raw_hex' in payload and payload['raw_hex']:
                raw_hex = payload['raw_hex'][4:]  # Skip first 2 bytes
            
            if not raw_hex:
                return
            
            # Decode and send packet
            await self.process_packet(raw_hex, payload)
            
        except Exception as e:
            self.logger.error(f"Error handling RX log data: {e}")
    
    def decode_packet(self, raw_hex: str) -> Optional[Dict[str, Any]]:
        """Decode a MeshCore packet.
        
        Args:
            raw_hex: Raw hex string of the packet.
            
        Returns:
            Decoded packet info or None if decoding fails.
        """
        try:
            if raw_hex.startswith('0x'):
                raw_hex = raw_hex[2:]
            
            byte_data = bytes.fromhex(raw_hex)
            
            if len(byte_data) < 2:
                return None
            
            header = byte_data[0]
            
            # Extract route type
            route_type = RouteType(header & 0x03)
            has_transport = route_type in [RouteType.TRANSPORT_FLOOD, RouteType.TRANSPORT_DIRECT]
            
            # Calculate offset based on transport codes
            offset = 1
            if has_transport and len(byte_data) >= 5:
                offset = 5
            
            if len(byte_data) <= offset:
                return None
            
            path_len = byte_data[offset]
            offset += 1
            
            if len(byte_data) < offset + path_len:
                return None
            
            # Extract path
            path_bytes = byte_data[offset:offset + path_len]
            path_hex = path_bytes.hex()
            
            # Extract payload type
            payload_type = PayloadType((header >> 2) & 0x0F)
            
            # Calculate packet hash
            packet_hash = calculate_packet_hash(raw_hex, payload_type.value)
            
            # Extract payload data (everything after path)
            payload_offset = offset + path_len
            payload_bytes_hex = byte_data[payload_offset:].hex() if payload_offset < len(byte_data) else ''
            
            return {
                'route_type': route_type.name,
                'payload_type': payload_type.name,
                'path_hex': path_hex,
                'path_len': path_len,
                'packet_hash': packet_hash,
                'payload_hex': payload_bytes_hex
            }
            
        except Exception as e:
            if self.debug:
                self.logger.debug(f"Error decoding packet: {e}")
            return None
    
    async def process_packet(self, raw_hex: str, payload: Dict[str, Any]) -> None:
        """Process and send packet to HBME API.
        
        Args:
            raw_hex: Raw hex string of the packet.
            payload: Payload dictionary from the event.
        """
        try:
            # Decode packet
            packet_info = self.decode_packet(raw_hex)
            if not packet_info:
                return
            
            # Build API payload (matches IngestorPacketIn schema)
            api_payload = {
                'route_type': self.ROUTE_TYPE_MAP.get(packet_info['route_type'], 'ROUTE_TYPE_FLOOD'),
                'payload_type': self.PAYLOAD_TYPE_MAP.get(packet_info['payload_type'], 'PAYLOAD_TYPE_REQ'),
                'snr': float(payload.get('snr', 0)),
                'rssi': int(payload.get('rssi', 0)),
                'path': packet_info['path_hex'],
                'path_len': int(packet_info['path_len']),
                'hash': packet_info['packet_hash'],
                'raw_hex': raw_hex if raw_hex else None
            }
            
            # Add timestamp for preview
            preview_entry = {
                'timestamp': datetime.now().isoformat(),
                'data': api_payload,
                'sent': False
            }
            
            # Always add to preview queue
            self.preview_queue.append(preview_entry)
            
            # Sync preview mode from DB (in case changed via WebUI)
            db_preview = self.bot.db_manager.get_metadata('hbme_ingestor_preview_mode')
            if db_preview is not None:
                self.preview_mode = (db_preview == 'true')
            
            # If preview mode, don't send to API
            if self.preview_mode:
                if self.debug:
                    self.logger.debug(f"Preview mode - captured packet: {api_payload['payload_type']}")
                # Save to DB for WebViewer
                self._save_stats_to_db()
                return
            
            # Reload credentials from DB in case updated via WebUI
            db_username = self.bot.db_manager.get_metadata('hbme_ingestor_username')
            db_password = self.bot.db_manager.get_metadata('hbme_ingestor_password')
            if db_username:
                self.username = db_username
            if db_password:
                self.password = db_password
            
            # Ensure authenticated
            if not self._authenticated:
                auth_ok = await self.authenticate()
                if not auth_ok:
                    self.logger.warning("Cannot send packet - authentication failed")
                    self.stats['packets_failed'] += 1
                    self._save_stats_to_db()
                    return
            
            # Send to API
            success = await self.send_to_api(api_payload, self.api_url)
            preview_entry['sent'] = success
            
            # Save to DB for WebViewer
            self._save_stats_to_db()
            
        except Exception as e:
            self.logger.error(f"Error processing packet: {e}")
    
    async def authenticate(self) -> bool:
        """Authenticate with Authelia SSO.
        
        Posts username/password to Authelia's first-factor endpoint
        and stores the session cookie for subsequent requests.
        
        Returns:
            True if authentication succeeded, False otherwise.
        """
        if not self.username or not self.password:
            self.logger.warning("Cannot authenticate: no username/password configured")
            return False
        
        if not self._session or self._session.closed:
            self._auth_cookie_jar = aiohttp.CookieJar(unsafe=True)
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                cookie_jar=self._auth_cookie_jar
            )
        
        auth_payload = {
            'username': self.username,
            'password': self.password,
            'keepMeLoggedIn': True,
            'targetURL': self.api_url,
            'requestMethod': 'POST'
        }
        
        try:
            self.logger.info(f"Authenticating with Authelia at {self.auth_url}...")
            
            async with self._session.post(
                self.auth_url,
                json=auth_payload,
                headers={'Content-Type': 'application/json'}
            ) as response:
                response_text = await response.text()
                
                if response.status == 200:
                    # Extract authelia_session cookie for cross-subdomain use
                    self._session_cookie = self._extract_session_cookie(response)
                    
                    try:
                        result = json.loads(response_text)
                        status = result.get('status', '')
                        if status == 'OK':
                            self._authenticated = True
                            self._auth_retry_count = 0
                            self.logger.info(f"Authelia authentication successful (cookie: {'yes' if self._session_cookie else 'no'})")
                            return True
                        else:
                            self.logger.warning(f"Authelia auth returned status: {status}")
                            self._authenticated = False
                            return False
                    except json.JSONDecodeError:
                        # Some setups just return 200 with a cookie
                        self._authenticated = True
                        self._auth_retry_count = 0
                        self.logger.info("Authelia authentication successful (cookie-based)")
                        return True
                elif response.status == 401:
                    self.logger.error("Authelia authentication failed: invalid credentials")
                    self.stats['last_error'] = 'Anmeldung fehlgeschlagen: Ungültige Zugangsdaten'
                    self.stats['last_error_time'] = datetime.now().isoformat()
                    self._authenticated = False
                    return False
                else:
                    self.logger.error(f"Authelia auth error: HTTP {response.status} - {response_text[:200]}")
                    self.stats['last_error'] = f'Authelia Fehler: HTTP {response.status}'
                    self.stats['last_error_time'] = datetime.now().isoformat()
                    self._authenticated = False
                    return False
                    
        except asyncio.TimeoutError:
            self.logger.error("Authelia authentication timeout")
            self.stats['last_error'] = 'Authelia Timeout'
            self.stats['last_error_time'] = datetime.now().isoformat()
            return False
        except Exception as e:
            self.logger.error(f"Authelia authentication error: {e}")
            self.stats['last_error'] = f'Authelia Fehler: {str(e)[:100]}'
            self.stats['last_error_time'] = datetime.now().isoformat()
            return False
    
    @staticmethod
    def _extract_session_cookie(response) -> Optional[str]:
        """Extract authelia_session cookie value from response.
        
        Needed because aiohttp CookieJar doesn't share cookies across
        subdomains (auth.hbme.sh → api.hbme.sh). We extract the value
        and pass it manually in the Cookie header.
        """
        # Try Set-Cookie headers
        for cookie_header in response.headers.getall('Set-Cookie', []):
            if 'authelia_session' in cookie_header:
                # Parse "authelia_session=VALUE; Path=/; ..."
                for part in cookie_header.split(';'):
                    part = part.strip()
                    if part.startswith('authelia_session='):
                        return part.split('=', 1)[1]
        
        # Try response cookies
        for cookie in response.cookies.values():
            if cookie.key == 'authelia_session':
                return cookie.value
        
        return None
    
    async def send_to_api(self, data: Dict[str, Any], api_url: str = None) -> bool:
        """Send packet data to an API endpoint.
        
        Args:
            data: Packet data to send.
            api_url: Target URL (defaults to primary api_url).
            
        Returns:
            True if successful, False otherwise.
        """
        if not self._session or self._session.closed:
            return False
        
        target_url = api_url or self.api_url
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Explicitly pass session cookie for cross-subdomain requests
        if self._session_cookie:
            headers['Cookie'] = f'authelia_session={self._session_cookie}'
        
        for attempt in range(self.max_retries):
            try:
                start_time = time.time()
                
                async with self._session.post(
                    target_url,
                    json=data,
                    headers=headers
                ) as response:
                    elapsed = time.time() - start_time
                    self.stats['api_response_times'].append(elapsed)
                    
                    if response.status == 200:
                        self.stats['packets_sent'] += 1
                        self.stats['last_send_time'] = datetime.now().isoformat()
                        
                        if self.debug:
                            self.logger.debug(f"Packet sent to {target_url} ({elapsed:.2f}s): {data['payload_type']}")
                        
                        return True
                    elif response.status in (401, 403, 302):
                        # Session expired or not authenticated - re-authenticate
                        self.logger.warning(f"Auth expired (HTTP {response.status}), re-authenticating...")
                        self._authenticated = False
                        self._auth_retry_count += 1
                        
                        if self._auth_retry_count <= self._max_auth_retries:
                            auth_ok = await self.authenticate()
                            if auth_ok:
                                continue  # Retry the request with new session
                        
                        error_text = await response.text()
                        self.stats['last_error'] = f"Anmeldung fehlgeschlagen (HTTP {response.status})"
                        self.stats['last_error_time'] = datetime.now().isoformat()
                        self.logger.error(f"Authentication failed after retries")
                        break
                    else:
                        error_text = await response.text()
                        self.stats['last_error'] = f"HTTP {response.status}: {error_text[:100]}"
                        self.stats['last_error_time'] = datetime.now().isoformat()
                        
                        self.logger.warning(f"API error: {response.status} - {error_text[:100]}")
                        
            except asyncio.TimeoutError:
                self.stats['last_error'] = "Timeout"
                self.stats['last_error_time'] = datetime.now().isoformat()
                self.logger.warning(f"API timeout (attempt {attempt + 1}/{self.max_retries})")
                
            except aiohttp.ClientError as e:
                self.stats['last_error'] = str(e)[:100]
                self.stats['last_error_time'] = datetime.now().isoformat()
                self.logger.warning(f"API client error: {e}")
                
            except Exception as e:
                self.stats['last_error'] = str(e)[:100]
                self.stats['last_error_time'] = datetime.now().isoformat()
                self.logger.error(f"API unexpected error: {e}")
            
            if attempt < self.max_retries - 1:
                await asyncio.sleep(self.retry_delay)
        
        self.stats['packets_failed'] += 1
        return False
    
    async def test_connection(self, api_url: str = None, username: str = None, password: str = None, auth_url: str = None) -> Dict[str, Any]:
        """Test API connection with Authelia SSO authentication.
        
        Args:
            api_url: Optional API URL to test (uses configured if None).
            username: Optional username to test (uses configured if None).
            password: Optional password to test (uses configured if None).
            auth_url: Optional Authelia URL to test (uses configured if None).
            
        Returns:
            Dict with success status, message, and response time.
        """
        test_url = api_url or self.api_url
        test_user = username or self.username
        test_pass = password or self.password
        test_auth_url = auth_url or self.auth_url
        
        if not test_user or not test_pass:
            return {
                'success': False,
                'message': 'Keine Zugangsdaten konfiguriert',
                'response_time': 0
            }
        
        # Try to use a real packet from preview queue for more realistic test
        test_payload = None
        if self.preview_queue:
            recent_packet = self.preview_queue[-1]
            if 'data' in recent_packet:
                test_payload = recent_packet['data'].copy()
        
        # Fallback to synthetic test payload
        if not test_payload:
            import hashlib
            test_hash = hashlib.sha256(f"test_{time.time()}".encode()).hexdigest()
            test_payload = {
                'route_type': 'ROUTE_TYPE_FLOOD',
                'payload_type': 'PAYLOAD_TYPE_ADVERT',
                'snr': -5.0,
                'rssi': -95,
                'path': 'aabbcc',
                'path_len': 3,
                'hash': test_hash
            }
        
        try:
            cookie_jar = aiohttp.CookieJar(unsafe=True)
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                cookie_jar=cookie_jar
            ) as session:
                # Step 1: Authenticate with Authelia
                auth_payload = {
                    'username': test_user,
                    'password': test_pass,
                    'keepMeLoggedIn': False,
                    'targetURL': test_url,
                    'requestMethod': 'POST'
                }
                
                start_time = time.time()
                
                async with session.post(
                    test_auth_url,
                    json=auth_payload,
                    headers={'Content-Type': 'application/json'}
                ) as auth_response:
                    auth_elapsed = time.time() - start_time
                    
                    if auth_response.status == 401:
                        return {
                            'success': False,
                            'message': 'Anmeldung fehlgeschlagen: Ungültige Zugangsdaten',
                            'response_time': round(auth_elapsed * 1000)
                        }
                    elif auth_response.status != 200:
                        auth_text = await auth_response.text()
                        return {
                            'success': False,
                            'message': f'Authelia Fehler: HTTP {auth_response.status}',
                            'response_time': round(auth_elapsed * 1000)
                        }
                    
                    # Extract authelia_session cookie for cross-subdomain use
                    session_cookie = self._extract_session_cookie(auth_response)
                    
                    # Check Authelia response
                    auth_text = await auth_response.text()
                    try:
                        auth_result = json.loads(auth_text)
                        auth_status = auth_result.get('status', '')
                        if auth_status != 'OK' and auth_status != '':
                            return {
                                'success': False,
                                'message': f'Authelia Status: {auth_status}',
                                'response_time': round(auth_elapsed * 1000)
                            }
                    except json.JSONDecodeError:
                        pass  # Cookie-based auth, no JSON response
                
                # Step 2: Send test packet with session cookie (explicit Cookie header)
                api_headers = {'Content-Type': 'application/json'}
                if session_cookie:
                    api_headers['Cookie'] = f'authelia_session={session_cookie}'
                
                start_time = time.time()
                
                async with session.post(
                    test_url,
                    json=test_payload,
                    headers=api_headers
                ) as response:
                    elapsed = time.time() - start_time
                    response_text = await response.text()
                    
                    if response.status == 200:
                        return {
                            'success': True,
                            'message': f'Anmeldung & Verbindung erfolgreich (HTTP {response.status})',
                            'response_time': round((auth_elapsed + elapsed) * 1000),
                            'response': response_text[:200]
                        }
                    elif response.status == 400 and 'duplicat' in response_text.lower():
                        return {
                            'success': True,
                            'message': 'Verbindung erfolgreich (Testpaket war Duplikat)',
                            'response_time': round((auth_elapsed + elapsed) * 1000)
                        }
                    elif response.status in (401, 403):
                        return {
                            'success': False,
                            'message': 'Anmeldung OK, aber API-Zugriff verweigert',
                            'response_time': round((auth_elapsed + elapsed) * 1000)
                        }
                    else:
                        return {
                            'success': False,
                            'message': f'API-Fehler: HTTP {response.status}',
                            'response_time': round((auth_elapsed + elapsed) * 1000),
                            'response': response_text[:200]
                        }
                        
        except asyncio.TimeoutError:
            return {
                'success': False,
                'message': 'Timeout - Server antwortet nicht',
                'response_time': 10000
            }
        except aiohttp.ClientError as e:
            return {
                'success': False,
                'message': f'Verbindungsfehler: {str(e)[:100]}',
                'response_time': 0
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'Fehler: {str(e)[:100]}',
                'response_time': 0
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get service statistics.
        
        Returns:
            Dictionary with service statistics.
        """
        avg_response_time = 0
        if self.stats['api_response_times']:
            avg_response_time = sum(self.stats['api_response_times']) / len(self.stats['api_response_times'])
        
        return {
            'enabled': self.enabled,
            'running': self._running,
            'preview_mode': self.preview_mode,
            'api_url': self.api_url,
            'auth_url': self.auth_url,
            'has_credentials': bool(self.username and self.password),
            'username': self.username,
            'authenticated': self._authenticated,
            'packets_sent': self.stats['packets_sent'],
            'packets_failed': self.stats['packets_failed'],
            'packets_captured': len(self.preview_queue),
            'last_send_time': self.stats['last_send_time'],
            'last_error': self.stats['last_error'],
            'last_error_time': self.stats['last_error_time'],
            'avg_response_time_ms': round(avg_response_time * 1000, 2)
        }
    
    def get_preview_packets(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent packets from preview queue.
        
        Args:
            limit: Maximum number of packets to return.
            
        Returns:
            List of recent packets (newest first).
        """
        packets = list(self.preview_queue)
        packets.reverse()  # Newest first
        return packets[:limit]
    
    def clear_preview_queue(self) -> int:
        """Clear the preview queue.
        
        Returns:
            Number of packets cleared.
        """
        count = len(self.preview_queue)
        self.preview_queue.clear()
        return count
    
    def set_preview_mode(self, enabled: bool) -> None:
        """Set preview mode.
        
        Args:
            enabled: True for preview mode, False for live mode.
        """
        self.preview_mode = enabled
        self.bot.db_manager.set_metadata('hbme_ingestor_preview_mode', 'true' if enabled else 'false')
        self.logger.info(f"HBME Ingestor preview mode: {'enabled' if enabled else 'disabled'}")
    
    def clear_last_error(self) -> None:
        """Clear the last error from stats and database."""
        self.stats['last_error'] = None
        self.stats['last_error_time'] = None
        self.bot.db_manager.set_metadata('hbme_stats_last_error', '')
        self.bot.db_manager.set_metadata('hbme_stats_last_error_time', '')
        self.logger.info("HBME Ingestor: Last error cleared")
