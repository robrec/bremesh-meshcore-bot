# MeshCore Bot

A Python bot that connects to MeshCore mesh networks via serial port, BLE, or TCP/IP. The bot responds to messages containing configured keywords, executes commands, and provides various data services including weather, solar conditions, and satellite pass information.

> **This is the BreMesh fork** — maintained by [Bartzi](https://github.com/robrec) with additional features for packet analysis, MQTT Decryption, and HBME-BIA integration. See [Fork Changes](#bremesh-fork-changes) below.

## Features

- **Connection Methods**: Serial port, BLE (Bluetooth Low Energy), or TCP/IP
- **Keyword Responses**: Configurable keyword-response pairs with template variables
- **Command System**: Plugin-based command architecture with built-in commands
- **Rate Limiting**: Global, per-user (by pubkey or name), and bot transmission rate limits to prevent spam
- **User Management**: Ban/unban users with persistent storage
- **Scheduled Messages**: Send messages at configured times
- **Direct Message Support**: Respond to private messages
- **Logging**: Console and file logging with configurable levels

### Service Plugins

- **Discord Bridge**(it's dead jimmy): One-way webhook bridge to post mesh messages to Discord ([docs](docs/discord-bridge.md))
- **Packet Capture**: Capture and publish packets to MQTT brokers with deep payload decoding ([docs](docs/packet-capture.md))
- **HBME Ingestor**: Forward packets to the HBME API for centralized mesh analysis ([docs](docs/hbme-ingestor.md))
- **Map Uploader**(wtf why?): Upload node adverts to map.meshcore.dev ([docs](docs/map-uploader.md))
- **Telemetry Monitor**: Poll repeater battery/environment data via MQTT with web UI ([docs](#telemetry-monitor-service))
- **Weather Service**: Scheduled forecasts, alerts, and lightning detection ([docs](docs/weather-service.md))

## Requirements

- Python 3.7+
- MeshCore-compatible device (Heltec V3, RAK Wireless, etc.)
- USB cable or BLE capability

## Installation

### Quick Start (Development)
1. Clone the repository:
```bash
git clone <repository-url>
cd meshcore-bot
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Copy and configure the bot:

**BreMesh Configuration**: Copy the pre-configured BreMesh config as your starting point:
```bash
cp config.hbmesh.ini config.ini
# Edit config.ini with your settings
```

4. Run the bot:
```bash
python3 meshcore_bot.py
```

### Production Installation (Systemd Service)
For production deployment as a system service:

1. Install as systemd service:
```bash
sudo ./install-service.sh
```

2. Configure the bot:
```bash
sudo nano /opt/meshcore-bot/config.ini
```

3. Start the service:
```bash
sudo systemctl start meshcore-bot
```

4. Check status:
```bash
sudo systemctl status meshcore-bot
```

See [Service installation](docs/service-installation.md) for detailed service installation instructions.

### Docker Deployment
For containerized deployment using Docker:

1. **Create data directories and configuration**:
   ```bash
   mkdir -p data/{config,databases,logs,backups}
   cp config.ini.example data/config/config.ini
   # Edit data/config/config.ini with your settings
   ```

2. **Update paths in config.ini** to use `/data/` directories:
   ```ini
   [Bot]
   db_path = /data/databases/meshcore_bot.db
   
   [Logging]
   log_file = /data/logs/meshcore_bot.log
   ```

3. **Build and start with Docker Compose**:
   ```bash
   docker compose build
   docker compose up -d
   ```
   
   Or build and start in one command:
   ```bash
   docker compose up -d --build
   ```

4. **View logs**:
   ```bash
   docker-compose logs -f
   ```

See [Docker deployment](docs/docker.md) for detailed Docker deployment instructions, including serial port access, web viewer configuration, and troubleshooting.

## NixOS
Use the Nix flake via flake.nix
```nix
meshcore-bot.url = "github:agessaman/meshcore-bot/";
```

And in your system config

```nix
{
  imports = [inputs.meshcore-bot.nixosModules.default];
  services.meshcore-bot = {
    enable = true;
    webviewer.enable = true;
    settings = {
      Connection.connection_type = "serial";
      Connection.serial_port = "/dev/ttyUSB0";
      Bot.bot_name = "MyBot";
    };
  };
}
```

## Configuration

The bot uses `config.ini` for all settings. Key configuration sections:

### Connection
```ini
[Connection]
connection_type = serial          # serial, ble, or tcp
serial_port = /dev/ttyUSB0        # Serial port path (for serial)
#hostname = 192.168.1.60         # TCP hostname/IP (for TCP)
#tcp_port = 5000                  # TCP port (for TCP)
#ble_device_name = MeshCore       # BLE device name (for BLE)
timeout = 30                      # Connection timeout
```

### Bot Settings
```ini
[Bot]
bot_name = MeshCoreBot            # Bot identification name
enabled = true                    # Enable/disable bot
rate_limit_seconds = 2            # Global: min seconds between any bot reply
bot_tx_rate_limit_seconds = 1.0   # Min seconds between bot transmissions
per_user_rate_limit_seconds = 5   # Per-user: min seconds between replies to same user (pubkey or name)
per_user_rate_limit_enabled = true
startup_advert = flood            # Send advert on startup
```

### Keywords
```ini
[Keywords]
# Format: keyword = response_template
# Variables: {sender}, {connection_info}, {snr}, {timestamp}, {path}
test = "Message received from {sender} | {connection_info}"
help = "Bot Help: test, ping, help, hello, cmd, wx, aqi, sun, moon, solar, hfcond, satpass, dice, roll, joke, dadjoke, sports, channels, path, prefix, repeater, stats, alert"
```

### Channels
```ini
[Channels]
monitor_channels = general,test,emergency  # Channels to monitor
respond_to_dms = true                      # Enable DM responses
# Optional: limit channel responses to certain keywords (DM gets all triggers)
# channel_keywords = help,ping,test,hello
```

### External Data APIs
```ini
[External_Data]
# API keys for external services
n2yo_api_key =                    # Satellite pass data
airnow_api_key =                  # Air quality data
```

### Alert Command
```ini
[Alert_Command]
enabled = true                           # Enable/disable alert command
max_incident_age_hours = 24             # Maximum age for incidents (hours)
max_distance_km = 20.0                  # Maximum distance for proximity queries (km)
agency.city.<city_name> = <agency_ids>   # City-specific agency IDs (e.g., agency.city.seattle = 17D20,17M15)
agency.county.<county_name> = <agency_ids> # County-specific agency IDs (aggregates all city agencies)
```

### Logging
```ini
[Logging]
log_level = INFO                  # DEBUG, INFO, WARNING, ERROR, CRITICAL
log_file = meshcore_bot.log       # Log file path
colored_output = true             # Enable colored console output
```

## Usage

### Running the Bot

```bash
python meshcore_bot.py
```

### Available Commands

For a comprehensive list of all available commands with examples and detailed explanations, see [Command reference](docs/command-reference.md).

Quick reference:
- **Basic:** `test`, `ping`, `help`, `hello`, `cmd`
- **Information:** `wx`, `gwx`, `aqi`, `sun`, `moon`, `solar`, `solarforecast`, `hfcond`, `satpass`, `channels`
- **Emergency:** `alert`
- **Gaming:** `dice`, `roll`, `magic8`
- **Entertainment:** `joke`, `dadjoke`, `hacker`, `catfact`
- **Sports:** `sports`
- **MeshCore Utility:** `path`, `prefix`, `stats`, `multitest`, `webviewer`
- **Management (DM only):** `repeater`, `advert`, `feed`, `announcements`, `greeter`

## Message Response Templates

Keyword responses support these template variables:

- `{sender}` - Sender's node ID
- `{connection_info}` - Connection details (direct/routed)
- `{snr}` - Signal-to-noise ratio
- `{timestamp}` - Message timestamp
- `{path}` - Message routing path

### Adding Newlines

To add newlines in keyword responses, use `\n` (single backslash + n):

```ini
[Keywords]
test = "Line 1\nLine 2\nLine 3"
```

This will output:
```
Line 1
Line 2
Line 3
```

To use a literal backslash + n, use `\\n` (double backslash + n).  
Other escape sequences: `\t` (tab), `\r` (carriage return), `\\` (literal backslash)

Example:
```ini
[Keywords]
test = "Message received from {sender} | {connection_info}"
ping = "Pong!"
help = "Bot Help: test, ping, help, hello, cmd, wx, gwx, aqi, sun, moon, solar, solarforecast, hfcond, satpass, dice, roll, joke, dadjoke, sports, channels, path, prefix, repeater, stats, multitest, alert, webviewer"
```

## Hardware Setup

### Serial Connection

1. Flash MeshCore firmware to your device
2. Connect via USB
3. Configure serial port in `config.ini`:
   ```ini
   [Connection]
   connection_type = serial
   serial_port = /dev/ttyUSB0  # Linux
   # serial_port = COM3        # Windows
   # serial_port = /dev/tty.usbserial-*  # macOS
   ```

### BLE Connection

1. Ensure your MeshCore device supports BLE
2. Configure BLE in `config.ini`:
   ```ini
   [Connection]
   connection_type = ble
   ble_device_name = MeshCore
   ```

### TCP Connection

1. Ensure your MeshCore device has TCP/IP connectivity (e.g., via gateway or bridge)
2. Configure TCP in `config.ini`:
   ```ini
   [Connection]
   connection_type = tcp
   hostname = 192.168.1.60  # IP address or hostname
   tcp_port = 5000          # TCP port (default: 5000)
   ```

## Troubleshooting

### Common Issues

1. **Serial Port Not Found**:
   - Check device connection
   - Verify port name in config
   - List available ports: `python -c "import serial.tools.list_ports; print([p.device for p in serial.tools.list_ports.comports()])"`

2. **BLE Connection Issues**:
   - Ensure device is discoverable
   - Check device name in config
   - Verify BLE permissions

3. **TCP Connection Issues**:
   - Verify hostname/IP address is correct
   - Check that TCP port is open and accessible
   - Ensure network connectivity to the device
   - Verify the MeshCore device supports TCP connections
   - Check firewall settings if connection fails

4. **Message Parsing Errors**:
   - Enable DEBUG logging for detailed information
   - Check meshcore library documentation for protocol details

5. **Rate Limiting**:
   - **Global**: `rate_limit_seconds` — minimum time between any two bot replies
   - **Per-user**: `per_user_rate_limit_seconds` and `per_user_rate_limit_enabled` — minimum time between replies to the same user (user identified by public key when available, else sender name; channel senders often matched by name)
   - **Bot TX**: `bot_tx_rate_limit_seconds` — minimum time between bot transmissions on the mesh
   - Check logs for rate limiting messages

### Debug Mode

Enable debug logging:
```ini
[Logging]
log_level = DEBUG
```

## Architecture

The bot uses a modular plugin architecture:

- **Core modules** (`modules/`): Shared utilities and core functionality
- **Command plugins** (`modules/commands/`): Individual command implementations
- **Service plugins** (`modules/service_plugins/`): Background services (Discord bridge, packet capture, etc.)
- **Plugin loaders**: Dynamic discovery and loading of command and service plugins
- **Message handler**: Processes incoming messages and routes to appropriate handlers

### Adding New Plugins

**Command Plugin:**
1. Create a new file in `modules/commands/`
2. Inherit from `BaseCommand`
3. Implement the `execute()` method
4. The plugin loader will automatically discover and load it

```python
from .base_command import BaseCommand
from ..models import MeshMessage

class MyCommand(BaseCommand):
    name = "mycommand"
    keywords = ['mycommand']
    description = "My custom command"

    async def execute(self, message: MeshMessage) -> bool:
        await self.send_response(message, "Hello from my command!")
        return True
```

**Service Plugin:**
1. Create a new file in `modules/service_plugins/`
2. Inherit from `BaseServicePlugin`
3. Implement `start()` and `stop()` methods
4. Add configuration section to `config.ini.example`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request against the dev branch

## License

This project is licensed under the MIT License.

---

## BreMesh Fork Changes

This fork adds the following features on top of upstream meshcore-bot:

### Enriched MQTT Payloads (Packet Capture)

Every packet published to MQTT now includes a `decoded` field with structured, human-readable data extracted from the raw packet bytes:

- **Header**: route type, payload type, version, path nodes
- **ADVERT**: public key, device name, role (Companion/Repeater/RoomServer/Sensor), GPS coordinates, advert timestamp
- **TXT_MSG**: decoded plain text content
- **GRP_TXT**: decrypted channel messages for public hashtag channels (AES-128-ECB + HMAC verification)
- **ACK**: ack hash
- **REQ/RESPONSE/PATH/TRACE**: payload size summaries

**Example decoded MQTT message:**
```json
{
  "origin": "6EF10422",
  "SNR": "13.5",
  "RSSI": "-51",
  "decoded": {
    "route_type_name": "FLOOD",
    "payload_type_name": "GRP_TXT",
    "path_nodes": ["ef", "fc", "17", "b0"],
    "group_text": {
      "encrypted": false,
      "channel_name": "#ping",
      "sender": "Ritter Fips",
      "message": "ping",
      "timestamp_iso": "2026-03-01T19:09:00Z"
    }
  }
}
```

See [Packet Capture docs](docs/packet-capture.md) for configuration.

### MQTT Subscribe: Nachrichten ins Mesh senden

Der Bot kann Nachrichten via MQTT empfangen und ins Mesh-Netzwerk senden. So lässt sich der Bot z.B. aus Node-RED, Home Assistant oder eigenen Skripten steuern.

**Konfiguration** in `config.ini` unter `[PacketCapture]`:

```ini
mqtt_subscribe_enabled = true
mqtt1_topic_send_dm = meshcore/send/dm
mqtt1_topic_send_channel = meshcore/send/channel
```

**DM senden** (Topic: `meshcore/send/dm`):
```json
{"destination": "NodeName", "message": "Hallo!"}
```

**Channel-Nachricht senden** (Topic: `meshcore/send/channel`):
```json
{"channel": "#ping", "message": "Pong! Test via MQTT"}
```

**Test via Kommandozeile:**
```bash
# Channel-Nachricht
mosquitto_pub -h localhost -t meshcore/send/channel -m '{"channel": "#ping", "message": "Test via MQTT"}'

# Direktnachricht
mosquitto_pub -h localhost -t meshcore/send/dm -m '{"destination": "MeinNode", "message": "Hallo!"}'
```

### Node-RED Demo Flows

Die folgenden Flows zeigen, wie man den Bot aus Node-RED heraus steuert. Importiere sie über **Menu → Import → Clipboard** in Node-RED.

<details>
<summary><b>Flow 1: Channel-Nachricht senden (Inject → MQTT)</b></summary>

Sendet eine Nachricht auf einen MeshCore-Channel per Knopfdruck.

```json
[
    {
        "id": "mesh_channel_flow",
        "type": "tab",
        "label": "MeshCore Channel",
        "disabled": false
    },
    {
        "id": "inject_channel",
        "type": "inject",
        "z": "mesh_channel_flow",
        "name": "Sende #ping",
        "props": [{"p": "payload", "vt": "json"}],
        "repeat": "",
        "once": false,
        "payload": "{\"channel\":\"#ping\",\"message\":\"Pong! Gesendet via Node-RED\"}",
        "payloadType": "json",
        "x": 170,
        "y": 100,
        "wires": [["mqtt_out_channel"]]
    },
    {
        "id": "mqtt_out_channel",
        "type": "mqtt out",
        "z": "mesh_channel_flow",
        "name": "MeshCore Channel",
        "topic": "meshcore/send/channel",
        "qos": "1",
        "retain": "",
        "broker": "mqtt_broker_local",
        "x": 420,
        "y": 100,
        "wires": []
    },
    {
        "id": "mqtt_broker_local",
        "type": "mqtt-broker",
        "name": "Mosquitto Lokal",
        "broker": "localhost",
        "port": "1883",
        "clientid": "nodered-meshcore",
        "autoConnect": true,
        "keepalive": "60",
        "cleansession": true
    }
]
```
</details>

<details>
<summary><b>Flow 2: Direktnachricht (DM) senden</b></summary>

Sendet eine DM an einen bestimmten Node im Mesh-Netzwerk.

```json
[
    {
        "id": "mesh_dm_flow",
        "type": "tab",
        "label": "MeshCore DM",
        "disabled": false
    },
    {
        "id": "inject_dm",
        "type": "inject",
        "z": "mesh_dm_flow",
        "name": "Sende DM",
        "props": [{"p": "payload", "vt": "json"}],
        "repeat": "",
        "once": false,
        "payload": "{\"destination\":\"MeinNode\",\"message\":\"Hallo aus Node-RED!\"}",
        "payloadType": "json",
        "x": 170,
        "y": 100,
        "wires": [["mqtt_out_dm"]]
    },
    {
        "id": "mqtt_out_dm",
        "type": "mqtt out",
        "z": "mesh_dm_flow",
        "name": "MeshCore DM",
        "topic": "meshcore/send/dm",
        "qos": "1",
        "retain": "",
        "broker": "mqtt_broker_local_dm",
        "x": 400,
        "y": 100,
        "wires": []
    },
    {
        "id": "mqtt_broker_local_dm",
        "type": "mqtt-broker",
        "name": "Mosquitto Lokal",
        "broker": "localhost",
        "port": "1883",
        "clientid": "nodered-meshcore-dm",
        "autoConnect": true,
        "keepalive": "60",
        "cleansession": true
    }
]
```
</details>

<details>
<summary><b>Flow 3: Dashboard-Formular → Mesh senden</b></summary>

Ein Node-RED Dashboard-Formular zum freien Eingeben von Channel, Empfänger und Nachricht. Wähle zwischen DM und Channel-Nachricht.

```json
[
    {
        "id": "mesh_dashboard_flow",
        "type": "tab",
        "label": "MeshCore Dashboard",
        "disabled": false
    },
    {
        "id": "inject_form",
        "type": "inject",
        "z": "mesh_dashboard_flow",
        "name": "Channel: #bremesh",
        "props": [{"p": "payload", "vt": "json"}, {"p": "topic", "vt": "str"}],
        "repeat": "",
        "once": false,
        "topic": "meshcore/send/channel",
        "payload": "{\"channel\":\"#bremesh\",\"message\":\"Moin aus Node-RED! 73\"}",
        "payloadType": "json",
        "x": 190,
        "y": 100,
        "wires": [["mqtt_out_dashboard"]]
    },
    {
        "id": "inject_form_dm",
        "type": "inject",
        "z": "mesh_dashboard_flow",
        "name": "DM: kleinBartzi",
        "props": [{"p": "payload", "vt": "json"}, {"p": "topic", "vt": "str"}],
        "repeat": "",
        "once": false,
        "topic": "meshcore/send/dm",
        "payload": "{\"destination\":\"kleinBartzi\",\"message\":\"Moin Bartzi, Grüße via Node-RED!\"}",
        "payloadType": "json",
        "x": 190,
        "y": 180,
        "wires": [["mqtt_out_dashboard"]]
    },
    {
        "id": "mqtt_out_dashboard",
        "type": "mqtt out",
        "z": "mesh_dashboard_flow",
        "name": "MeshCore Send",
        "topic": "",
        "qos": "1",
        "retain": "",
        "broker": "mqtt_broker_dashboard",
        "x": 450,
        "y": 140,
        "wires": []
    },
    {
        "id": "mqtt_broker_dashboard",
        "type": "mqtt-broker",
        "name": "Mosquitto Lokal",
        "broker": "localhost",
        "port": "1883",
        "clientid": "nodered-meshcore-dashboard",
        "autoConnect": true,
        "keepalive": "60",
        "cleansession": true
    }
]
```
</details>

<details>
<summary><b>Flow 4: Empfangene Pakete mitlesen (MQTT → Debug)</b></summary>

Empfängt alle Pakete vom Bot und zeigt sie im Debug-Panel an.

```json
[
    {
        "id": "mesh_monitor_flow",
        "type": "tab",
        "label": "MeshCore Monitor",
        "disabled": false
    },
    {
        "id": "mqtt_in_packets",
        "type": "mqtt in",
        "z": "mesh_monitor_flow",
        "name": "Mesh Pakete",
        "topic": "meshcore/loc/packets",
        "qos": "0",
        "datatype": "json",
        "broker": "mqtt_broker_monitor",
        "x": 170,
        "y": 100,
        "wires": [["debug_packets"]]
    },
    {
        "id": "mqtt_in_status",
        "type": "mqtt in",
        "z": "mesh_monitor_flow",
        "name": "Mesh Status",
        "topic": "meshcore/loc/status",
        "qos": "0",
        "datatype": "json",
        "broker": "mqtt_broker_monitor",
        "x": 170,
        "y": 180,
        "wires": [["debug_status"]]
    },
    {
        "id": "debug_packets",
        "type": "debug",
        "z": "mesh_monitor_flow",
        "name": "Pakete",
        "active": true,
        "tosidebar": true,
        "complete": "payload",
        "x": 390,
        "y": 100,
        "wires": []
    },
    {
        "id": "debug_status",
        "type": "debug",
        "z": "mesh_monitor_flow",
        "name": "Status",
        "active": true,
        "tosidebar": true,
        "complete": "payload",
        "x": 390,
        "y": 180,
        "wires": []
    },
    {
        "id": "mqtt_broker_monitor",
        "type": "mqtt-broker",
        "name": "Mosquitto Lokal",
        "broker": "localhost",
        "port": "1883",
        "clientid": "nodered-meshcore-monitor",
        "autoConnect": true,
        "keepalive": "60",
        "cleansession": true
    }
]
```
</details>

### GRP_TXT Channel Decryption

Public hashtag channels (e.g. `#ping`, `#CQ`, `Public`) use deterministic keys derived from the channel name. The packet capture service can decrypt these on the fly:

```ini
[PacketCapture]
decode_hashtag_channels = Public,#ping,#test,#emergency
```

- `Public` — the default MeshCore channel (case-sensitive, no `#` prefix)
- `#channelname` — hashtag channels with `#` prefix
- Key derivation: `SHA256(channel_name_as_bytes)[:16]`

### HBME Ingestor Service

Forwards captured packets to the [HBME API](https://hbme.sh) for centralized mesh network analysis. Features:

- Authelia SSO authentication
- Preview mode with packet queue (viewable in web UI)
- Live mode for production forwarding
- Real-time WebSocket updates in the services page

**Zugang erhalten:** Kontaktiere [@bartzi:hbme.sh](https://matrix.to/#/@bartzi:hbme.sh) via Matrix, um deinen Token für die Registrierung auf [register.hbme.sh](https://register.hbme.sh/) zu bekommen.

See [HBME Ingestor docs](docs/hbme-ingestor.md).

### Telemetry Monitor Service

Der Telemetry Monitor fragt die Batterie- und Umgebungsdaten (Temperatur, Luftfeuchtigkeit, Luftdruck, GPS) von Repeatern im Mesh-Netzwerk ab und speichert sie in einer SQLite-Datenbank. Ergebnisse werden auch via MQTT publiziert, sodass externe Systeme (Node-RED, Home Assistant, Grafana, etc.) die Daten in Echtzeit verarbeiten können.

> **⚠️ WICHTIG: Repeater ACL-Berechtigung erforderlich!**
>
> Der Bot kann **nur dann Telemetriedaten** von einem Repeater abfragen, wenn er in der **Access Control List (ACL)** des Repeaters als **Read/Write** oder **Admin** eingetragen ist!
>
> Ohne diese Berechtigung wird der Telemetrie-Request vom Repeater ignoriert und der Bot erhält keine Antwort (Timeout).
>
> **So fügst du den Bot zur Repeater-ACL hinzu:**
> 1. Verbinde dich mit dem Repeater über die MeshCore Companion App
> 2. Öffne die Kontaktliste des Repeaters
> 3. Suche den Bot-Kontakt (z.B. "TestBot")
> 4. Setze die Berechtigung auf **Read/Write** oder **Admin**

#### Konfiguration

```ini
[MQTT]
server = localhost
port = 1883
transport = tcp
use_tls = false
use_auth_token = false

[TelemetryMonitor]
enabled = true
poll_interval_minutes = 180
request_timeout = 60
max_retries = 3
default_path_mode = flood
database_path = telemetry_data.db
# MQTT aktivieren für externe Systeme
mqtt_enabled = true
mqtt_topic_request = meshcore/telemetry/request
mqtt_topic_response = meshcore/telemetry/response
```

#### MQTT Telemetrie-Request senden

Sende eine JSON-Nachricht an das Request-Topic, um eine Telemetrie-Abfrage auszulösen:

**Topic:** `meshcore/telemetry/request`

```json
{"repeater": "RepeaterName", "path": "flood"}
```

| Feld | Pflicht | Beschreibung |
|------|---------|-------------|
| `repeater` | ✅ | Name des Repeaters (exakt wie in der Kontaktliste) |
| `path` | ❌ | Routing-Modus: `flood` (Default), `direct`, oder Hex-Path z.B. `ef,10` |

**Beispiele:**

```bash
# Flood-Routing (Default)
mosquitto_pub -h localhost -t meshcore/telemetry/request \
  -m '{"repeater": "Mitte @BreMesh", "path": "flood"}'

# Direct-Path (bekannter Pfad)
mosquitto_pub -h localhost -t meshcore/telemetry/request \
  -m '{"repeater": "Mitte @BreMesh", "path": "direct"}'

# Custom Hex-Path
mosquitto_pub -h localhost -t meshcore/telemetry/request \
  -m '{"repeater": "Mitte @BreMesh", "path": "ef,10"}'

# Minimal (ohne path, nutzt flood)
mosquitto_pub -h localhost -t meshcore/telemetry/request \
  -m '{"repeater": "Mitte @BreMesh"}'
```

#### MQTT Telemetrie-Response

Der Bot publiziert die Telemetriedaten auf dem Response-Topic. Jede Abfrage (automatisch oder via MQTT-Request) wird hier veröffentlicht – auch fehlgeschlagene Versuche, sodass man den Fortschritt in Echtzeit mitlesen kann.

**Topic:** `meshcore/telemetry/response`

**Erfolgreiche Abfrage:**
```json
{
  "repeater": "Mitte🔌 @BreMesh",
  "success": true,
  "path": "flood",
  "attempt": "1/3",
  "duration_ms": 2341,
  "temperature": 23.0,
  "humidity": null,
  "pressure": null,
  "battery_voltage": 4.34,
  "battery_percent": 100.0,
  "latitude": null,
  "longitude": null,
  "altitude": null
}
```

**Fehlgeschlagener Versuch (Retry):**
```json
{
  "repeater": "Mitte🔌 @BreMesh",
  "success": false,
  "path": "flood",
  "attempt": "1/3",
  "duration_ms": 60012,
  "error": "No telemetry response (timeout)"
}
```

**Automatischer Poll (Telemetrie-Reading):**
```json
{
  "repeater": "Mitte🔌 @BreMesh",
  "timestamp": "2026-03-08T17:18:52",
  "duration_ms": 2341,
  "temperature": 23.0,
  "humidity": null,
  "pressure": null,
  "battery_voltage": 4.34,
  "battery_percent": 100.0,
  "latitude": null,
  "longitude": null,
  "altitude": null,
  "raw_data": [
    {"channel": 1, "type": "voltage", "value": 4.34},
    {"channel": 1, "type": "temperature", "value": 23.0}
  ]
}
```

| Feld | Typ | Beschreibung |
|------|-----|-------------|
| `repeater` | string | Name des abgefragten Repeaters |
| `success` | bool | `true` bei Erfolg, `false` bei Timeout (nur bei MQTT-Request) |
| `attempt` | string | Aktueller Versuch, z.B. `"1/3"`, `"2/3"` (nur bei MQTT-Request) |
| `path` | string | Verwendeter Routing-Modus (nur bei MQTT-Request) |
| `timestamp` | string | ISO-Zeitstempel der Abfrage |
| `duration_ms` | int | Dauer der Abfrage in Millisekunden |
| `temperature` | float/null | Temperatur in °C |
| `humidity` | float/null | Luftfeuchtigkeit in % |
| `pressure` | float/null | Luftdruck in hPa |
| `battery_voltage` | float/null | Batteriespannung in V |
| `battery_percent` | float/null | Batteriestand in % (berechnet: 3.0V=0%, 4.2V=100%) |
| `latitude` | float/null | GPS Breitengrad |
| `longitude` | float/null | GPS Längengrad |
| `altitude` | float/null | Höhe in m |
| `raw_data` | array | Rohe LPP-Telemetriedaten vom Device (nur bei automatischem Poll) |
| `error` | string | Fehlermeldung (nur bei `success: false`) |

**Test via Kommandozeile:**
```bash
# Telemetrie-Responses mitlesen
mosquitto_sub -h localhost -t meshcore/telemetry/response -v

# Request senden und auf Antwort warten
mosquitto_sub -h localhost -t meshcore/telemetry/response &
mosquitto_pub -h localhost -t meshcore/telemetry/request \
  -m '{"repeater": "Mitte @BreMesh"}'
```

#### WebSocket Telemetrie-Stream

Die Telemetriedaten werden auch über den WebSocket des Web Viewers gestreamt (gleiche Datenstruktur wie MQTT). Clients können sich auf den `telemetry_data`-Event subscriben, um Echtzeit-Updates zu erhalten.

**Verbindung (JavaScript/Socket.IO):**
```javascript
const socket = io('http://localhost:8080');

// Telemetrie-Stream abonnieren
socket.emit('subscribe_telemetry');

// Telemetrie-Events empfangen
socket.on('telemetry_data', (data) => {
  console.log('Telemetrie:', data);
});
```

**Event:** `telemetry_data`

Es gibt zwei Typen von Events:

**`type: "reading"` — Erfolgreiche Messung:**
```json
{
  "type": "reading",
  "repeater": "Mitte🔌 @BreMesh",
  "timestamp": "2026-03-08 17:18:52",
  "duration_ms": 2341,
  "temperature": 23.0,
  "humidity": null,
  "pressure": null,
  "battery_voltage": 4.34,
  "battery_percent": 100.0,
  "latitude": null,
  "longitude": null,
  "altitude": null,
  "raw_data": [
    {"channel": 1, "type": "voltage", "value": 4.34},
    {"channel": 1, "type": "temperature", "value": 23.0}
  ]
}
```

**`type: "poll_attempt"` — Poll-Versuch (Erfolg oder Fehler):**
```json
{
  "type": "poll_attempt",
  "repeater": "Mitte🔌 @BreMesh",
  "timestamp": "2026-03-08 17:19:52",
  "success": false,
  "attempt": "1/3",
  "duration_ms": 60012,
  "path": "flood",
  "error": "No telemetry response (timeout)"
}
```

### Services Page (Web Viewer)

New `/services` page in the web viewer for managing service plugins:

- HBME Ingestor management (credentials, mode toggle, packet monitor)
- Real-time packet feed via WebSocket with HTTP polling fallback
- Dark theme support

See [Web Viewer docs](docs/web-viewer.md).

---

## Acknowledgments

- [MeshCore Project](https://github.com/meshcore-dev/MeshCore) for the mesh networking protocol
- Some commands adapted from MeshingAround bot by K7MHI Kelly Keeton 2024
- Packet capture service based on [meshcore-packet-capture](https://github.com/agessaman/meshcore-packet-capture) by agessaman
- [meshcore-decoder](https://github.com/michaelhart/meshcore-decoder) by Michael Hart for client-side packet decoding and decryption in the web viewer
- SWB sonst wäre es dunkel hier.
