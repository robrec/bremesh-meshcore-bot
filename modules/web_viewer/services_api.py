#!/usr/bin/env python3
"""
Services API Module
Handles API endpoints for external service integrations (HBME Ingestor, Telemetry, etc.)
"""

import asyncio
import json
import os
import sqlite3
from datetime import datetime
from typing import Optional
from flask import jsonify, request


class ServicesAPI:
    """Handles Services API endpoints for external integrations."""
    
    def __init__(self, app, db_manager, logger, config=None, bot_instance=None):
        """Initialize Services API.
        
        Args:
            app: Flask application instance
            db_manager: Database manager instance
            logger: Logger instance
            bot_instance: Optional bot instance for service access
        """
        self.app = app
        self.db_manager = db_manager
        self.logger = logger
        self.config = config
        self.bot = bot_instance
        self._register_routes()
    
    def set_bot_instance(self, bot):
        """Set bot instance for service access.
        
        Args:
            bot: The bot instance.
        """
        self.bot = bot
    
    def _register_routes(self):
        """Register all API routes."""
        
        # ─────────────────────────────────────────────────────────────────────
        # HBME Ingestor Service
        # ─────────────────────────────────────────────────────────────────────
        
        @self.app.route('/api/services/hbme/config')
        def api_hbme_config():
            """Get HBME Ingestor configuration."""
            return self._get_hbme_config()
        
        @self.app.route('/api/services/hbme/config', methods=['POST'])
        def api_hbme_save_config():
            """Save HBME Ingestor configuration."""
            return self._save_hbme_config()
        
        @self.app.route('/api/services/hbme/toggle', methods=['POST'])
        def api_hbme_toggle():
            """Toggle HBME Ingestor service."""
            return self._toggle_hbme()
        
        @self.app.route('/api/services/hbme/test', methods=['POST'])
        def api_hbme_test():
            """Test HBME Ingestor connection."""
            return self._test_hbme()
        
        @self.app.route('/api/services/hbme/stats')
        def api_hbme_stats():
            """Get HBME Ingestor statistics."""
            return self._get_hbme_stats()
        
        @self.app.route('/api/services/hbme/preview')
        def api_hbme_preview():
            """Get preview packets."""
            return self._get_hbme_preview()
        
        @self.app.route('/api/services/hbme/preview/clear', methods=['POST'])
        def api_hbme_preview_clear():
            """Clear preview queue."""
            return self._clear_hbme_preview()
        
        @self.app.route('/api/services/hbme/preview-mode', methods=['POST'])
        def api_hbme_preview_mode():
            """Toggle preview mode."""
            return self._toggle_hbme_preview_mode()
        
        @self.app.route('/api/services/hbme/clear-error', methods=['POST'])
        def api_hbme_clear_error():
            """Clear last error."""
            return self._clear_hbme_error()
        
        @self.app.route('/api/services/hbme/telemetry-forward')
        def api_hbme_telemetry_forward_config():
            """Get HBME telemetry forwarding config."""
            return self._get_hbme_telemetry_forward_config()
        
        @self.app.route('/api/services/hbme/telemetry-forward', methods=['POST'])
        def api_hbme_telemetry_forward_save():
            """Save HBME telemetry forwarding config."""
            return self._save_hbme_telemetry_forward_config()
        
        # ─────────────────────────────────────────────────────────────────────
        # Telemetry Monitor Service
        # ─────────────────────────────────────────────────────────────────────
        
        @self.app.route('/api/telemetry/status')
        def api_telemetry_status():
            """Get telemetry monitor service status."""
            return jsonify(self._get_telemetry_status())
        
        @self.app.route('/api/telemetry/readings')
        def api_telemetry_readings():
            """Get latest telemetry readings."""
            return jsonify({'readings': self._get_telemetry_readings()})
        
        @self.app.route('/api/telemetry/poll-history')
        def api_telemetry_poll_history():
            """Get recent poll attempts."""
            return jsonify({'history': self._get_telemetry_poll_history()})
        
        @self.app.route('/api/telemetry/repeaters')
        def api_telemetry_repeaters():
            """Get list of monitored repeaters."""
            return jsonify(self._get_monitored_repeaters())
        
        @self.app.route('/api/telemetry/repeaters', methods=['POST'])
        def api_telemetry_repeaters_add():
            """Add a new repeater to monitoring list."""
            data = request.get_json()
            if not data or not data.get('name'):
                return jsonify({'success': False, 'error': 'Name required'}), 400
            return jsonify(self._add_monitored_repeater(data['name'].strip()))
        
        @self.app.route('/api/telemetry/repeaters/<int:repeater_id>', methods=['DELETE'])
        def api_telemetry_repeaters_delete(repeater_id):
            """Remove a repeater from monitoring list."""
            return jsonify(self._delete_monitored_repeater(repeater_id))
        
        @self.app.route('/api/telemetry/repeaters/<int:repeater_id>/toggle', methods=['POST'])
        def api_telemetry_repeaters_toggle(repeater_id):
            """Toggle repeater enabled state."""
            return jsonify(self._toggle_monitored_repeater(repeater_id))
        
        @self.app.route('/api/telemetry/poll-now/<path:repeater_name>', methods=['POST'])
        def api_telemetry_poll_now(repeater_name):
            """Trigger ad-hoc poll for a specific repeater."""
            data = request.get_json(silent=True) or {}
            path_mode = data.get('path', 'flood')
            return jsonify(self._trigger_poll(repeater_name, path_mode))
        
        @self.app.route('/api/telemetry/poll-all', methods=['POST'])
        def api_telemetry_poll_all():
            """Trigger ad-hoc poll for all repeaters."""
            data = request.get_json(silent=True) or {}
            path_mode = data.get('path', 'flood')
            return jsonify(self._trigger_poll_all(path_mode))
        
        @self.app.route('/api/telemetry/webhook/config')
        def api_telemetry_webhook_config():
            """Get webhook configuration."""
            return jsonify(self._get_webhook_config())
        
        @self.app.route('/api/telemetry/webhook/config', methods=['POST'])
        def api_telemetry_webhook_save():
            """Save webhook configuration."""
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'error': 'No data'}), 400
            return jsonify(self._save_webhook_config(data))
        
        @self.app.route('/api/telemetry/webhook/test', methods=['POST'])
        def api_telemetry_webhook_test():
            """Test webhook with a test payload."""
            return jsonify(self._test_webhook())
        
        @self.app.route('/api/telemetry/webhook/log')
        def api_telemetry_webhook_log():
            """Get webhook log entries."""
            return jsonify({'log': self._get_webhook_log()})
        
        @self.app.route('/api/telemetry/available-contacts')
        def api_telemetry_available_contacts():
            """Get known contacts for repeater selection."""
            return jsonify({'contacts': self._get_available_contacts()})
        
        @self.app.route('/api/telemetry/poll-interval', methods=['POST'])
        def api_telemetry_poll_interval():
            """Update poll interval (minutes)."""
            data = request.get_json(silent=True) or {}
            return jsonify(self._save_poll_interval(data))
        
        @self.app.route('/api/telemetry/mqtt-config')
        def api_telemetry_mqtt_config():
            """Get MQTT config for telemetry."""
            cfg = self._get_telemetry_config()
            result = {
                'mqtt_enabled': cfg.get('mqtt_enabled', False),
                'mqtt_server': cfg.get('mqtt_server', 'localhost'),
                'mqtt_port': cfg.get('mqtt_port', 1883),
                'mqtt_topic_request': cfg.get('mqtt_topic_request', 'meshcore/telemetry/request'),
                'mqtt_topic_response': cfg.get('mqtt_topic_response', 'meshcore/telemetry/response'),
            }
            # Override with DB values if set
            try:
                db_path = self._get_telemetry_db_path()
                if os.path.exists(db_path):
                    conn = self._get_telemetry_db()
                    cursor = conn.cursor()
                    try:
                        cursor.execute('SELECT mqtt_enabled, mqtt_topic_request, mqtt_topic_response FROM service_status WHERE id = 1')
                        row = cursor.fetchone()
                        if row:
                            if row['mqtt_enabled'] is not None:
                                result['mqtt_enabled'] = row['mqtt_enabled'] == '1'
                            if row['mqtt_topic_request']:
                                result['mqtt_topic_request'] = row['mqtt_topic_request']
                            if row['mqtt_topic_response']:
                                result['mqtt_topic_response'] = row['mqtt_topic_response']
                    except sqlite3.OperationalError:
                        pass
                    conn.close()
            except Exception:
                pass
            return jsonify(result)
        
        @self.app.route('/api/telemetry/mqtt-config', methods=['POST'])
        def api_telemetry_mqtt_config_save():
            """Save MQTT config for telemetry."""
            data = request.get_json(silent=True) or {}
            return jsonify(self._save_mqtt_config(data))
        
        # ─────────────────────────────────────────────────────────────────────
        # General Services Overview
        # ─────────────────────────────────────────────────────────────────────
        
        @self.app.route('/api/services/overview')
        def api_services_overview():
            """Get overview of all external services."""
            return self._get_services_overview()
    
    # ─────────────────────────────────────────────────────────────────────────
    # HBME Ingestor Methods
    # ─────────────────────────────────────────────────────────────────────────
    
    def _get_hbme_config(self):
        """Get HBME Ingestor configuration."""
        try:
            # Get from database first, fallback to defaults
            enabled = self.db_manager.get_metadata('hbme_ingestor_enabled')
            api_url = self.db_manager.get_metadata('hbme_ingestor_api_url')
            auth_url = self.db_manager.get_metadata('hbme_ingestor_auth_url')
            username = self.db_manager.get_metadata('hbme_ingestor_username')
            password = self.db_manager.get_metadata('hbme_ingestor_password')
            preview_mode = self.db_manager.get_metadata('hbme_ingestor_preview_mode')
            telemetry_forward = self.db_manager.get_metadata('hbme_telemetry_forward_enabled')
            
            # Default values
            default_url = 'https://api.hbme.sh/ingestor/auth/packet'
            default_auth_url = 'https://auth.hbme.sh/api/firstfactor'
            
            return jsonify({
                'enabled': enabled == 'true' if enabled else False,
                'preview_mode': preview_mode == 'true' if preview_mode is not None else True,
                'telemetry_forward_enabled': telemetry_forward == 'true' if telemetry_forward else False,
                'api_url': api_url or default_url,
                'auth_url': auth_url or default_auth_url,
                'has_credentials': bool(username and password),
                'username': username or ''
            })
        except Exception as e:
            self.logger.error(f"Error getting HBME config: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _save_hbme_config(self):
        """Save HBME Ingestor configuration."""
        try:
            data = request.get_json()
            
            if not data:
                return jsonify({'error': 'No data provided'}), 400
            
            # Save API URL
            if 'api_url' in data:
                api_url = data['api_url'].strip()
                if api_url:
                    self.db_manager.set_metadata('hbme_ingestor_api_url', api_url)
            
            # Save Auth URL
            if 'auth_url' in data:
                auth_url = data['auth_url'].strip()
                if auth_url:
                    self.db_manager.set_metadata('hbme_ingestor_auth_url', auth_url)
            
            # Save Username
            if 'username' in data:
                username = data['username'].strip()
                if username:
                    self.db_manager.set_metadata('hbme_ingestor_username', username)
            
            # Save Password (only if provided, not empty)
            if 'password' in data:
                password = data['password'].strip()
                if password:
                    self.db_manager.set_metadata('hbme_ingestor_password', password)
            
            # Reload service config if running
            self._reload_hbme_service()
            
            return jsonify({
                'success': True,
                'message': 'Konfiguration gespeichert'
            })
        except Exception as e:
            self.logger.error(f"Error saving HBME config: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _toggle_hbme(self):
        """Toggle HBME Ingestor service."""
        try:
            data = request.get_json() or {}
            
            # Get current state
            current = self.db_manager.get_metadata('hbme_ingestor_enabled')
            current_enabled = current == 'true' if current else False
            
            # Toggle or set explicitly
            if 'enabled' in data:
                new_enabled = data['enabled']
            else:
                new_enabled = not current_enabled
            
            # Check if credentials exist before enabling
            if new_enabled:
                username = self.db_manager.get_metadata('hbme_ingestor_username')
                password = self.db_manager.get_metadata('hbme_ingestor_password')
                if not username or not password:
                    return jsonify({
                        'success': False,
                        'message': 'Bitte zuerst Zugangsdaten (Username & Passwort) konfigurieren',
                        'enabled': False
                    })
            
            # Save new state
            self.db_manager.set_metadata('hbme_ingestor_enabled', 'true' if new_enabled else 'false')
            
            # Start/stop service if bot is available
            if self.bot:
                try:
                    loop = self.bot.loop if hasattr(self.bot, 'loop') else asyncio.get_event_loop()
                    asyncio.run_coroutine_threadsafe(
                        self._manage_hbme_service(new_enabled),
                        loop
                    )
                except Exception as e:
                    self.logger.warning(f"Could not manage service lifecycle: {e}")
            
            return jsonify({
                'success': True,
                'enabled': new_enabled,
                'message': 'Service aktiviert' if new_enabled else 'Service deaktiviert'
            })
        except Exception as e:
            self.logger.error(f"Error toggling HBME service: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _test_hbme(self):
        """Test HBME Ingestor connection with Authelia SSO."""
        import aiohttp
        import time
        import hashlib
        
        try:
            data = request.get_json() or {}
            
            # Get test parameters
            api_url = data.get('api_url') or self.db_manager.get_metadata('hbme_ingestor_api_url')
            auth_url = data.get('auth_url') or self.db_manager.get_metadata('hbme_ingestor_auth_url')
            
            # Always get credentials from database - never from frontend
            username = self.db_manager.get_metadata('hbme_ingestor_username')
            password = self.db_manager.get_metadata('hbme_ingestor_password')
            
            if not api_url:
                api_url = 'https://api.hbme.sh/ingestor/auth/packet'
            if not auth_url:
                auth_url = 'https://auth.hbme.sh/api/firstfactor'
            
            if not username or not password:
                return jsonify({
                    'success': False,
                    'message': 'Keine Zugangsdaten konfiguriert',
                    'response_time': 0
                })
            
            # Try to get a real packet from preview queue in DB
            test_payload = None
            try:
                import json
                preview_data = self.db_manager.get_metadata('hbme_preview_queue')
                if preview_data:
                    packets = json.loads(preview_data)
                    if packets:
                        test_payload = packets[-1].get('data')
            except Exception as e:
                self.logger.debug(f"Could not load preview packet: {e}")
            
            # Fallback to synthetic payload
            if not test_payload:
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
            
            # Direct async test with Authelia SSO
            async def do_test():
                cookie_jar = aiohttp.CookieJar(unsafe=True)
                try:
                    async with aiohttp.ClientSession(
                        timeout=aiohttp.ClientTimeout(total=10),
                        cookie_jar=cookie_jar
                    ) as session:
                        # Step 1: Authenticate with Authelia
                        auth_payload = {
                            'username': username,
                            'password': password,
                            'keepMeLoggedIn': False
                        }
                        
                        start_time = time.time()
                        
                        async with session.post(
                            auth_url,
                            json=auth_payload,
                            headers={'Content-Type': 'application/json'}
                        ) as auth_response:
                            auth_elapsed = time.time() - start_time
                            
                            if auth_response.status == 401:
                                return {
                                    'success': False,
                                    'message': 'Anmeldung fehlgeschlagen: Ung\u00fcltige Zugangsdaten',
                                    'response_time': round(auth_elapsed * 1000)
                                }
                            elif auth_response.status != 200:
                                return {
                                    'success': False,
                                    'message': f'Authelia Fehler: HTTP {auth_response.status}',
                                    'response_time': round(auth_elapsed * 1000)
                                }
                            
                            # Check Authelia response
                            try:
                                import json as json_mod
                                auth_text = await auth_response.text()
                                auth_result = json_mod.loads(auth_text)
                                auth_status = auth_result.get('status', '')
                                if auth_status and auth_status != 'OK':
                                    return {
                                        'success': False,
                                        'message': f'Authelia Status: {auth_status}',
                                        'response_time': round(auth_elapsed * 1000)
                                    }
                            except (json.JSONDecodeError, Exception):
                                pass
                        
                        # Step 2: Send test packet with session cookie
                        start_time = time.time()
                        
                        async with session.post(
                            api_url,
                            json=test_payload,
                            headers={'Content-Type': 'application/json'}
                        ) as response:
                            elapsed = time.time() - start_time
                            response_text = await response.text()
                            
                            if response.status == 200:
                                return {
                                    'success': True,
                                    'message': 'Anmeldung & Verbindung erfolgreich',
                                    'response_time': round((auth_elapsed + elapsed) * 1000)
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
                                    'response_time': round((auth_elapsed + elapsed) * 1000)
                                }
                except asyncio.TimeoutError:
                    return {
                        'success': False,
                        'message': 'Timeout - Server antwortet nicht',
                        'response_time': 10000
                    }
                except Exception as e:
                    return {
                        'success': False,
                        'message': f'Verbindungsfehler: {str(e)[:100]}',
                        'response_time': 0
                    }
            
            # Run the async test
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(do_test())
            finally:
                loop.close()
            
            return jsonify(result)
                
        except Exception as e:
            self.logger.error(f"Error testing HBME connection: {e}")
            return jsonify({
                'success': False,
                'message': f'Fehler: {str(e)[:100]}',
                'response_time': 0
            })
    
    def _get_hbme_stats(self):
        """Get HBME Ingestor statistics."""
        try:
            service = self._get_hbme_service()
            
            if service:
                # Direct access to service (in-process)
                stats = service.get_stats()
                return jsonify(stats)
            else:
                # Read from database (cross-process communication)
                db = self.db_manager
                
                enabled = db.get_metadata('hbme_ingestor_enabled')
                username = db.get_metadata('hbme_ingestor_username')
                password = db.get_metadata('hbme_ingestor_password')
                api_url = db.get_metadata('hbme_ingestor_api_url')
                auth_url = db.get_metadata('hbme_ingestor_auth_url')
                
                # Read stats from DB
                packets_sent = db.get_metadata('hbme_stats_packets_sent')
                packets_failed = db.get_metadata('hbme_stats_packets_failed')
                packets_captured = db.get_metadata('hbme_stats_packets_captured')
                running = db.get_metadata('hbme_stats_running')
                avg_response_ms = db.get_metadata('hbme_stats_avg_response_ms')
                last_send_time = db.get_metadata('hbme_stats_last_send_time')
                last_error = db.get_metadata('hbme_stats_last_error')
                last_error_time = db.get_metadata('hbme_stats_last_error_time')
                preview_mode = db.get_metadata('hbme_ingestor_preview_mode')
                
                return jsonify({
                    'enabled': enabled == 'true' if enabled else False,
                    'running': running == 'true' if running else False,
                    'preview_mode': preview_mode == 'true' if preview_mode is not None else True,
                    'api_url': api_url or 'https://api.hbme.sh/ingestor/auth/packet',
                    'auth_url': auth_url or 'https://auth.hbme.sh/api/firstfactor',
                    'has_credentials': bool(username and password),
                    'username': username or '',
                    'packets_sent': int(packets_sent) if packets_sent else 0,
                    'packets_failed': int(packets_failed) if packets_failed else 0,
                    'packets_captured': int(packets_captured) if packets_captured else 0,
                    'last_send_time': last_send_time,
                    'last_error': last_error,
                    'last_error_time': last_error_time,
                    'avg_response_time_ms': float(avg_response_ms) if avg_response_ms else 0
                })
        except Exception as e:
            self.logger.error(f"Error getting HBME stats: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _get_hbme_service(self):
        """Get HBME Ingestor service instance from bot."""
        if not self.bot:
            return None
        
        # Check in services dict
        if hasattr(self.bot, 'services') and isinstance(self.bot.services, dict):
            if 'hbmeingestor' in self.bot.services:
                return self.bot.services['hbmeingestor']
            for name, service in self.bot.services.items():
                if service.__class__.__name__ == 'HBMEIngestorService':
                    return service
        
        # Fallback: check in service_plugins list (legacy)
        if hasattr(self.bot, 'service_plugins'):
            for service in self.bot.service_plugins:
                if service.__class__.__name__ == 'HBMEIngestorService':
                    return service
        
        return None
    
    def _reload_hbme_service(self):
        """Reload HBME service configuration."""
        service = self._get_hbme_service()
        if service:
            service.reload_config()
    
    async def _manage_hbme_service(self, enable: bool):
        """Start or stop HBME service."""
        service = self._get_hbme_service()
        if service:
            service.reload_config()
            if enable and not service._running:
                await service.start()
            elif not enable and service._running:
                await service.stop()
    
    def _get_hbme_telemetry_forward_config(self):
        """Get HBME telemetry forwarding configuration."""
        try:
            enabled = self.db_manager.get_metadata('hbme_telemetry_forward_enabled')
            return jsonify({
                'enabled': enabled == 'true' if enabled else False,
            })
        except Exception as e:
            self.logger.error(f"Error getting HBME telemetry forward config: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _save_hbme_telemetry_forward_config(self):
        """Save HBME telemetry forwarding configuration."""
        try:
            data = request.get_json()
            if data is None:
                return jsonify({'success': False, 'error': 'No data'}), 400
            
            enabled = bool(data.get('enabled', False))
            self.db_manager.set_metadata(
                'hbme_telemetry_forward_enabled', 'true' if enabled else 'false')
            
            return jsonify({
                'success': True,
                'enabled': enabled,
                'message': 'Telemetrie-Weiterleitung ' + ('aktiviert' if enabled else 'deaktiviert'),
            })
        except Exception as e:
            self.logger.error(f"Error saving HBME telemetry forward config: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _get_hbme_preview(self):
        """Get preview packets from HBME Ingestor."""
        try:
            import json
            limit = request.args.get('limit', 20, type=int)
            service = self._get_hbme_service()
            
            if service:
                packets = service.get_preview_packets(limit)
                return jsonify({
                    'packets': packets,
                    'total': len(service.preview_queue),
                    'preview_mode': service.preview_mode
                })
            else:
                # Read from database (cross-process)
                db = self.db_manager
                preview_queue_json = db.get_metadata('hbme_preview_queue')
                preview_mode = db.get_metadata('hbme_ingestor_preview_mode')
                
                packets = []
                if preview_queue_json:
                    try:
                        all_packets = json.loads(preview_queue_json)
                        packets = list(reversed(all_packets))[:limit]
                    except json.JSONDecodeError:
                        pass
                
                return jsonify({
                    'packets': packets,
                    'total': len(packets),
                    'preview_mode': preview_mode == 'true' if preview_mode is not None else True
                })
        except Exception as e:
            self.logger.error(f"Error getting HBME preview: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _clear_hbme_preview(self):
        """Clear HBME preview queue."""
        try:
            service = self._get_hbme_service()
            
            if service:
                count = service.clear_preview_queue()
                self.db_manager.set_metadata('hbme_preview_queue', '[]')
                return jsonify({
                    'success': True,
                    'message': f'{count} Pakete gelöscht'
                })
            else:
                self.db_manager.set_metadata('hbme_preview_queue', '[]')
                return jsonify({
                    'success': True,
                    'message': 'Queue in Datenbank geleert'
                })
        except Exception as e:
            self.logger.error(f"Error clearing HBME preview: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _toggle_hbme_preview_mode(self):
        """Toggle HBME preview mode."""
        try:
            data = request.get_json() or {}
            service = self._get_hbme_service()
            
            # Get current state from DB
            current = self.db_manager.get_metadata('hbme_ingestor_preview_mode')
            current_preview = current == 'true' if current is not None else True
            
            # Toggle or set explicitly
            if 'preview_mode' in data:
                new_preview = data['preview_mode']
            else:
                new_preview = not current_preview
            
            # Save to DB (single source of truth)
            self.db_manager.set_metadata('hbme_ingestor_preview_mode', 'true' if new_preview else 'false')
            
            # Update service if running
            if service:
                service.set_preview_mode(new_preview)
            
            return jsonify({
                'success': True,
                'preview_mode': new_preview,
                'message': 'TEST-Modus aktiviert' if new_preview else 'LIVE-Modus aktiviert - Pakete werden gesendet!'
            })
        except Exception as e:
            self.logger.error(f"Error toggling HBME preview mode: {e}")
            return jsonify({'error': str(e)}), 500
    
    def _clear_hbme_error(self):
        """Clear the last error from HBME Ingestor."""
        try:
            service = self._get_hbme_service()
            
            if service:
                service.clear_last_error()
            else:
                self.db_manager.set_metadata('hbme_stats_last_error', '')
                self.db_manager.set_metadata('hbme_stats_last_error_time', '')
            
            return jsonify({
                'success': True,
                'message': 'Letzter Fehler gelöscht'
            })
        except Exception as e:
            self.logger.error(f"Error clearing HBME error: {e}")
            return jsonify({'error': str(e)}), 500
    
    # ─────────────────────────────────────────────────────────────────────────
    # General Overview Methods
    # ─────────────────────────────────────────────────────────────────────────
    
    def _get_services_overview(self):
        """Get overview of all external services."""
        try:
            services = []
            db = self.db_manager
            
            # HBME Ingestor
            hbme_enabled = db.get_metadata('hbme_ingestor_enabled')
            hbme_running = db.get_metadata('hbme_stats_running')
            hbme_service = self._get_hbme_service()
            
            running = False
            if hbme_service:
                running = hbme_service._running
            elif hbme_running:
                running = hbme_running == 'true'
            
            services.append({
                'id': 'hbme',
                'name': 'HBME Paket Ingestor',
                'description': 'Sendet Paketdaten an die HBME API für Netzwerkanalyse',
                'enabled': hbme_enabled == 'true' if hbme_enabled else False,
                'running': running,
                'icon': 'fa-broadcast-tower'
            })
            
            # Telemetry Monitor
            telemetry_status = self._get_telemetry_status()
            services.append({
                'id': 'telemetry',
                'name': 'Telemetry Monitor',
                'description': 'Fragt Repeater-Telemetrie ab (Batterie, Temperatur, etc.)',
                'enabled': telemetry_status.get('enabled', False),
                'running': telemetry_status.get('running', False),
                'icon': 'fa-battery-half'
            })
            
            return jsonify({
                'services': services,
                'total': len(services),
                'active': sum(1 for s in services if s['enabled'])
            })
        except Exception as e:
            self.logger.error(f"Error getting services overview: {e}")
            return jsonify({'error': str(e)}), 500
    
    # ─────────────────────────────────────────────────────────────────────────
    # Telemetry Monitor Methods
    # ─────────────────────────────────────────────────────────────────────────
    
    def _get_telemetry_db_path(self) -> str:
        """Resolve the telemetry database path from config."""
        import configparser
        config = configparser.ConfigParser()
        # Get the config path from the app instance
        config_path = getattr(self.app, '_config_path', None)
        if not config_path:
            # Fallback: resolve from bot root
            bot_root = os.path.join(os.path.dirname(__file__), '..', '..')
            config_path = os.path.join(bot_root, 'config.ini')
        config.read(config_path, encoding='utf-8')
        
        db_path = config.get('TelemetryMonitor', 'database_path', fallback='telemetry_data.db')
        if not os.path.isabs(db_path):
            bot_root = os.path.join(os.path.dirname(__file__), '..', '..')
            db_path = os.path.join(os.path.abspath(bot_root), db_path)
        return db_path
    
    def _get_telemetry_db(self) -> sqlite3.Connection:
        """Open a connection to the telemetry database."""
        db_path = self._get_telemetry_db_path()
        conn = sqlite3.connect(db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _get_telemetry_config(self) -> dict:
        """Read TelemetryMonitor config section."""
        import configparser
        config = configparser.ConfigParser()
        config_path = getattr(self.app, '_config_path', None)
        if not config_path:
            bot_root = os.path.join(os.path.dirname(__file__), '..', '..')
            config_path = os.path.join(bot_root, 'config.ini')
        config.read(config_path, encoding='utf-8')
        
        if not config.has_section('TelemetryMonitor'):
            return {'enabled': False}
        
        return {
            'enabled': config.getboolean('TelemetryMonitor', 'enabled', fallback=False),
            'poll_interval_minutes': config.getint('TelemetryMonitor', 'poll_interval_minutes', fallback=30),
            'request_timeout': config.getint('TelemetryMonitor', 'request_timeout', fallback=60),
            'max_retries': config.getint('TelemetryMonitor', 'max_retries', fallback=3),
            'default_path_mode': config.get('TelemetryMonitor', 'default_path_mode', fallback='flood'),
            'mqtt_enabled': config.getboolean('TelemetryMonitor', 'mqtt_enabled', fallback=False),
            'mqtt_server': config.get('MQTT', 'server', fallback='localhost'),
            'mqtt_port': config.getint('MQTT', 'port', fallback=1883),
            'mqtt_topic_request': config.get('TelemetryMonitor', 'mqtt_topic_request', fallback='meshcore/telemetry/request'),
            'mqtt_topic_response': config.get('TelemetryMonitor', 'mqtt_topic_response', fallback='meshcore/telemetry/response'),
        }
    
    def _get_telemetry_status(self) -> dict:
        """Get telemetry monitor service status."""
        status = {
            'enabled': False,
            'running': False,
            'repeaters': [],
            'poll_interval_minutes': 30,
            'next_poll': None,
            'current_status': 'unknown',
            'current_repeater': None,
            'current_attempt': None,
            'max_attempts': 3,
            'repeaters_completed': 0,
            'repeaters_total': 0,
            'last_poll_time': None,
            'total_readings': 0,
            'successful_polls': 0,
            'failed_polls': 0,
            'mqtt_enabled': False,
            'webhook_enabled': False,
        }
        
        try:
            cfg = self._get_telemetry_config()
            status['enabled'] = cfg.get('enabled', False)
            status['poll_interval_minutes'] = cfg.get('poll_interval_minutes', 30)
            status['mqtt_enabled'] = cfg.get('mqtt_enabled', False)
            status['running'] = cfg.get('enabled', False)
            
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return status
            
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            
            # Repeaters
            try:
                cursor.execute('SELECT name FROM monitored_repeaters WHERE enabled = 1 ORDER BY sort_order, id')
                status['repeaters'] = [row['name'] for row in cursor.fetchall()]
            except sqlite3.OperationalError:
                pass
            
            # Live status
            try:
                cursor.execute('''
                    SELECT status, current_repeater, current_attempt, max_attempts,
                           last_update, next_poll_time, repeaters_completed, repeaters_total
                    FROM service_status WHERE id = 1
                ''')
                row = cursor.fetchone()
                if row:
                    status['current_status'] = row['status']
                    status['current_repeater'] = row['current_repeater']
                    status['current_attempt'] = row['current_attempt']
                    status['max_attempts'] = row['max_attempts'] or 3
                    status['next_poll'] = row['next_poll_time']
                    status['repeaters_completed'] = row['repeaters_completed'] or 0
                    status['repeaters_total'] = row['repeaters_total'] or len(status['repeaters'])
            except sqlite3.OperationalError:
                pass
            
            # Runtime poll interval from DB
            try:
                cursor.execute('SELECT poll_interval_minutes FROM service_status WHERE id = 1')
                row = cursor.fetchone()
                if row and row['poll_interval_minutes'] is not None:
                    status['poll_interval_minutes'] = row['poll_interval_minutes']
            except sqlite3.OperationalError:
                pass
            
            # Stats
            try:
                cursor.execute('SELECT COUNT(*) as cnt FROM telemetry_readings')
                status['total_readings'] = cursor.fetchone()['cnt']
            except sqlite3.OperationalError:
                pass
            
            try:
                cursor.execute('''
                    SELECT
                        SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as ok,
                        SUM(CASE WHEN success = 0 AND attempt_number >= max_attempts THEN 1 ELSE 0 END) as failed,
                        MAX(timestamp) as last_poll
                    FROM poll_attempts
                ''')
                row = cursor.fetchone()
                if row:
                    status['successful_polls'] = row['ok'] or 0
                    status['failed_polls'] = row['failed'] or 0
                    status['last_poll_time'] = row['last_poll']
            except sqlite3.OperationalError:
                pass
            
            # Webhook status
            try:
                cursor.execute('SELECT enabled FROM webhook_config WHERE id = 1')
                wh = cursor.fetchone()
                if wh:
                    status['webhook_enabled'] = bool(wh['enabled'])
            except sqlite3.OperationalError:
                pass
            
            conn.close()
        except Exception as e:
            self.logger.error(f"Error getting telemetry status: {e}")
        
        return status
    
    def _save_mqtt_config(self, data: dict) -> dict:
        """Save MQTT telemetry topic config to telemetry DB (picked up by service)."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Telemetry DB nicht initialisiert'}
            
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            
            # Ensure columns exist (migration)
            for col in ('mqtt_enabled', 'mqtt_topic_request', 'mqtt_topic_response'):
                try:
                    cursor.execute(f'SELECT {col} FROM service_status LIMIT 1')
                except sqlite3.OperationalError:
                    default = "''" if 'topic' in col else '0'
                    cursor.execute(f'ALTER TABLE service_status ADD COLUMN {col} TEXT DEFAULT {default}')
            
            sets = []
            vals = []
            if 'mqtt_enabled' in data:
                sets.append('mqtt_enabled = ?')
                vals.append('1' if data['mqtt_enabled'] else '0')
            if 'mqtt_topic_request' in data:
                topic = data['mqtt_topic_request'].strip()
                if topic:
                    sets.append('mqtt_topic_request = ?')
                    vals.append(topic)
            if 'mqtt_topic_response' in data:
                topic = data['mqtt_topic_response'].strip()
                if topic:
                    sets.append('mqtt_topic_response = ?')
                    vals.append(topic)
            
            if not sets:
                return {'success': False, 'error': 'Keine Änderungen'}
            
            cursor.execute(f'UPDATE service_status SET {", ".join(sets)} WHERE id = 1', vals)
            conn.commit()
            conn.close()
            return {'success': True, 'message': 'MQTT-Konfiguration gespeichert (wird beim nächsten Zyklus übernommen)'}
        except Exception as e:
            self.logger.error(f"Error saving MQTT config: {e}")
            return {'success': False, 'error': str(e)}

    def _save_poll_interval(self, data: dict) -> dict:
        """Save poll interval to telemetry DB (picked up by service at next cycle)."""
        try:
            minutes = int(data.get('minutes', 0))
            if minutes < 0:
                return {'success': False, 'error': 'Intervall darf nicht negativ sein'}
            if minutes > 1440:
                return {'success': False, 'error': 'Intervall darf maximal 1440 Minuten (24h) sein'}
            
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Telemetry DB nicht initialisiert'}
            
            conn = self._get_telemetry_db()
            conn.execute(
                'UPDATE service_status SET poll_interval_minutes = ? WHERE id = 1',
                (minutes,))
            conn.commit()
            conn.close()
            if minutes == 0:
                return {'success': True, 'message': 'Automatisches Polling deaktiviert (nur MQTT/Ad-hoc)'}
            return {'success': True, 'message': f'Poll-Intervall auf {minutes} Minuten gesetzt'}
        except Exception as e:
            self.logger.error(f"Error saving poll interval: {e}")
            return {'success': False, 'error': str(e)}

    def _get_telemetry_readings(self) -> list:
        """Get latest telemetry readings."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return []
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT t1.* FROM telemetry_readings t1
                INNER JOIN (
                    SELECT repeater_name, MAX(timestamp) as max_ts
                    FROM telemetry_readings WHERE success = 1
                    GROUP BY repeater_name
                ) t2 ON t1.repeater_name = t2.repeater_name AND t1.timestamp = t2.max_ts
                ORDER BY t1.repeater_name
            ''')
            readings = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return readings
        except Exception as e:
            self.logger.error(f"Error getting telemetry readings: {e}")
            return []
    
    def _get_telemetry_poll_history(self, limit: int = 50) -> list:
        """Get recent poll attempts."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return []
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM poll_attempts ORDER BY timestamp DESC LIMIT ?', (limit,))
            history = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return history
        except Exception as e:
            self.logger.error(f"Error getting poll history: {e}")
            return []
    
    def _get_monitored_repeaters(self) -> dict:
        """Get list of monitored repeaters from DB."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'repeaters': [], 'success': True}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM monitored_repeaters ORDER BY sort_order, id')
            repeaters = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return {'repeaters': repeaters, 'success': True}
        except Exception as e:
            self.logger.error(f"Error getting repeaters: {e}")
            return {'repeaters': [], 'success': False, 'error': str(e)}
    
    def _resolve_public_key(self, name: str) -> Optional[str]:
        """Resolve a repeater's public_key from the bot's contact list."""
        if not self.bot or not hasattr(self.bot, 'meshcore') or not self.bot.meshcore:
            return None
        try:
            contact = self.bot.meshcore.get_contact_by_name(name)
            if not contact:
                contacts = self.bot.meshcore.contacts if hasattr(self.bot.meshcore, 'contacts') else []
                for c in contacts:
                    c_name = c.get('adv_name', '') or c.get('name', '') if isinstance(c, dict) else (
                        getattr(c, 'adv_name', '') or getattr(c, 'name', ''))
                    if name.lower() in c_name.lower():
                        contact = c
                        break
            if contact:
                key = (contact.get('public_key') if isinstance(contact, dict)
                       else getattr(contact, 'public_key', None))
                return key or None
        except Exception:
            pass
        return None

    def _add_monitored_repeater(self, name: str) -> dict:
        """Add a new repeater to monitoring list."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Telemetry DB not initialized. Enable service first.'}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            # Get max sort_order
            cursor.execute('SELECT COALESCE(MAX(sort_order), 0) + 1 as next_order FROM monitored_repeaters')
            next_order = cursor.fetchone()['next_order']
            # Auto-resolve public_key from contact list
            public_key = self._resolve_public_key(name)
            cursor.execute(
                'INSERT INTO monitored_repeaters (name, sort_order, public_key) VALUES (?, ?, ?)',
                (name, next_order, public_key))
            conn.commit()
            repeater_id = cursor.lastrowid
            conn.close()
            return {'success': True, 'id': repeater_id, 'message': f'Repeater "{name}" hinzugefuegt'}
        except sqlite3.IntegrityError:
            return {'success': False, 'error': f'Repeater "{name}" existiert bereits'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _delete_monitored_repeater(self, repeater_id: int) -> dict:
        """Remove a repeater from monitoring list."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'DB not found'}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('DELETE FROM monitored_repeaters WHERE id = ?', (repeater_id,))
            conn.commit()
            deleted = cursor.rowcount > 0
            conn.close()
            return {'success': deleted}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _toggle_monitored_repeater(self, repeater_id: int) -> dict:
        """Toggle repeater enabled state."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'DB not found'}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE monitored_repeaters
                SET enabled = CASE WHEN enabled = 1 THEN 0 ELSE 1 END
                WHERE id = ?
            ''', (repeater_id,))
            conn.commit()
            cursor.execute('SELECT enabled FROM monitored_repeaters WHERE id = ?', (repeater_id,))
            row = cursor.fetchone()
            conn.close()
            if row:
                return {'success': True, 'enabled': bool(row['enabled'])}
            return {'success': False, 'error': 'Not found'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _trigger_poll(self, repeater_name: str, path_mode: str = 'flood') -> dict:
        """Insert an ad-hoc poll request for a single repeater."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Telemetry DB not found'}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
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
                INSERT INTO adhoc_poll_requests (repeater_name, status, path_mode, source)
                VALUES (?, 'pending', ?, 'webui')
            ''', (repeater_name, path_mode))
            conn.commit()
            req_id = cursor.lastrowid
            conn.close()
            return {'success': True, 'request_id': req_id,
                    'message': f'Poll gestartet fuer {repeater_name}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _trigger_poll_all(self, path_mode: str = 'flood') -> dict:
        """Insert ad-hoc poll requests for all enabled repeaters."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Telemetry DB not found'}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('SELECT name FROM monitored_repeaters WHERE enabled = 1 ORDER BY sort_order, id')
            repeaters = [row['name'] for row in cursor.fetchall()]
            if not repeaters:
                conn.close()
                return {'success': False, 'error': 'Keine Repeater konfiguriert'}
            request_ids = []
            for name in repeaters:
                cursor.execute('''
                    INSERT INTO adhoc_poll_requests (repeater_name, status, path_mode, source)
                    VALUES (?, 'pending', ?, 'webui')
                ''', (name, path_mode))
                request_ids.append(cursor.lastrowid)
            conn.commit()
            conn.close()
            return {'success': True, 'repeaters': repeaters, 'request_ids': request_ids,
                    'message': f'Poll gestartet fuer {len(repeaters)} Repeater'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _get_webhook_config(self) -> dict:
        """Get webhook configuration from telemetry DB."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'enabled': False, 'url': '', 'interval_minutes': 0,
                        'on_threshold': True, 'battery_threshold': 20.0}
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            try:
                cursor.execute('SELECT * FROM webhook_config WHERE id = 1')
                row = cursor.fetchone()
                conn.close()
                if row:
                    return {
                        'enabled': bool(row['enabled']),
                        'url': row['url'] or '',
                        'interval_minutes': row['interval_minutes'] or 0,
                        'on_threshold': bool(row['on_threshold']),
                        'battery_threshold': row['battery_threshold'] or 20.0,
                        'last_sent': row['last_sent'],
                        'last_error': row['last_error'],
                    }
            except sqlite3.OperationalError:
                conn.close()
            return {'enabled': False, 'url': '', 'interval_minutes': 0,
                    'on_threshold': True, 'battery_threshold': 20.0}
        except Exception as e:
            self.logger.error(f"Error getting webhook config: {e}")
            return {'enabled': False, 'url': '', 'interval_minutes': 0,
                    'on_threshold': True, 'battery_threshold': 20.0}
    
    def _save_webhook_config(self, data: dict) -> dict:
        """Save webhook configuration to telemetry DB."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Telemetry DB not initialized'}
            
            # Validate URL if provided
            url = data.get('url', '').strip()
            if url and not url.startswith(('http://', 'https://')):
                return {'success': False, 'error': 'URL muss mit http:// oder https:// beginnen'}
            
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE webhook_config SET
                    enabled = ?,
                    url = ?,
                    interval_minutes = ?,
                    on_threshold = ?,
                    battery_threshold = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            ''', (
                int(data.get('enabled', False)),
                url,
                int(data.get('interval_minutes', 0)),
                int(data.get('on_threshold', True)),
                float(data.get('battery_threshold', 20.0)),
            ))
            conn.commit()
            conn.close()
            return {'success': True, 'message': 'Webhook-Konfiguration gespeichert'}
        except Exception as e:
            self.logger.error(f"Error saving webhook config: {e}")
            return {'success': False, 'error': str(e)}
    
    def _test_webhook(self) -> dict:
        """Send a test payload to the configured webhook URL."""
        import time
        try:
            wh = self._get_webhook_config()
            url = wh.get('url', '').strip()
            if not url:
                return {'success': False, 'error': 'Keine Webhook-URL konfiguriert'}
            
            test_payload = {
                'trigger': 'test',
                'triggered_by': 'WebUI Test',
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'readings': [],
                'message': 'This is a test webhook from MeshCore Bot Telemetry Monitor',
            }
            
            import urllib.request
            body = json.dumps(test_payload).encode('utf-8')
            req = urllib.request.Request(
                url, data=body,
                headers={'Content-Type': 'application/json'},
                method='POST')
            
            start = time.time()
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    status_code = resp.getcode()
                    elapsed = int((time.time() - start) * 1000)
                    success = 200 <= status_code < 300
                    return {
                        'success': success,
                        'status_code': status_code,
                        'response_time': elapsed,
                        'message': f'HTTP {status_code} ({elapsed}ms)' if success else f'Fehler: HTTP {status_code}'
                    }
            except Exception as e:
                elapsed = int((time.time() - start) * 1000)
                return {'success': False, 'error': str(e), 'response_time': elapsed}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _get_webhook_log(self, limit: int = 20) -> list:
        """Get recent webhook log entries."""
        try:
            db_path = self._get_telemetry_db_path()
            if not os.path.exists(db_path):
                return []
            conn = self._get_telemetry_db()
            cursor = conn.cursor()
            try:
                cursor.execute('SELECT * FROM webhook_log ORDER BY timestamp DESC LIMIT ?', (limit,))
                log = [dict(row) for row in cursor.fetchall()]
            except sqlite3.OperationalError:
                log = []
            conn.close()
            return log
        except Exception as e:
            self.logger.error(f"Error getting webhook log: {e}")
            return []
    
    def _get_available_contacts(self) -> list:
        """Get known contacts for repeater selection dropdown."""
        try:
            contacts = []
            # From bot DB complete_contact_tracking
            db_path = self.db_manager.db_path if hasattr(self.db_manager, 'db_path') else None
            if db_path and os.path.exists(db_path):
                conn = sqlite3.connect(db_path, timeout=5.0)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                try:
                    cursor.execute('''
                        SELECT DISTINCT name, public_key
                        FROM complete_contact_tracking
                        WHERE name IS NOT NULL AND name != ''
                        ORDER BY name
                    ''')
                    for row in cursor.fetchall():
                        contacts.append({
                            'name': row['name'],
                            'public_key': row['public_key'][:12] + '...' if row['public_key'] else None
                        })
                except sqlite3.OperationalError:
                    pass
                conn.close()
            return contacts
        except Exception as e:
            self.logger.error(f"Error getting available contacts: {e}")
            return []
