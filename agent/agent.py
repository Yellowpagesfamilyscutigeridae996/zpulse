#!/usr/bin/env python3
# ZPulse - Developed by acidvegas in Python (https://github.com/acidvegas/rackwatch)
# zpulse/agent/agent.py

import argparse
import asyncio
import json
import logging
import os
import re
import shutil
import socket
import subprocess
import threading
import time

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime           import datetime
from pathlib            import Path

try:
	import apv
except ImportError:
	raise ImportError('missing apv module (pip install apv)')

try:
	import websockets
except ImportError:
	raise ImportError('missing websockets module (pip install websockets)')

# ── Configuration ────────────────────────────────────────────────────────────

SMART_INTERVAL = 300
POOL_INTERVAL  = 30
IO_INTERVAL    = 3

HEALTH_PENALTIES = {
	5:   (30, 3), # Reallocated Sectors
	10:  (15, 5), # Spin Retry Count
	187: (20, 2), # Reported Uncorrectable Errors
	188: (10, 1), # Command Timeout
	196: (10, 2), # Reallocation Event Count
	197: (30, 5), # Current Pending Sector Count
	198: (30, 5), # Offline Uncorrectable
	199: (10, 1), # UDMA CRC Error Count
}

# ── Global State ─────────────────────────────────────────────────────────────

lock         = threading.Lock()
init_done    = threading.Event()
capabilities = {'smartctl': False, 'zfs': False, 'hostname': socket.gethostname()}

cache = {
	'disks'       : [],
	'pools'       : [],
	'datasets'    : [],
	'snapshots'   : [],
	'io_rates'    : {},
	'pool_map'    : {},
	'system_info' : {},
}

_prev_diskstats = {}


# ── Helpers ──────────────────────────────────────────────────────────────────

def run_cmd(cmd: list[str], timeout: int = 30):
	'''
	Run a shell command and return stdout, stderr, and return code.

	:param cmd: Command and arguments to execute
	:param timeout: Maximum seconds to wait before killing the process
	'''

	try:
		r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
		return r.stdout, r.stderr, r.returncode
	except subprocess.TimeoutExpired:
		return '', 'timeout', -1
	except FileNotFoundError:
		return '', f'{cmd[0]} not found', -2
	except Exception as e:
		return '', str(e), -3


def detect_capabilities():
	'''Check system for smartctl and zpool availability.'''

	capabilities['smartctl'] = shutil.which('smartctl') is not None
	capabilities['zfs']      = shutil.which('zpool')    is not None


# ── System Info ──────────────────────────────────────────────────────────────

def collect_dimm_info():
	'''Collect DIMM slot details from dmidecode.'''

	out, _, rc = run_cmd(['dmidecode', '-t', 'memory'])
	if rc != 0:
		return []
	dimms = []
	for block in out.split('Memory Device')[1:]:
		d = {}
		for line in block.splitlines():
			line = line.strip()
			if ':' not in line:
				continue
			key, val = line.split(':', 1)
			d[key.strip()] = val.strip()
		if not d:
			continue
		size_str = d.get('Size', '')
		populated = bool(size_str) and 'No Module Installed' not in size_str
		dimms.append({
			'slot'         : d.get('Locator', ''),
			'size'         : size_str if populated else '',
			'type'         : d.get('Type', ''),
			'speed'        : d.get('Speed', ''),
			'configured_speed' : d.get('Configured Memory Speed', d.get('Configured Clock Speed', '')),
			'manufacturer' : d.get('Manufacturer', ''),
			'part_number'  : d.get('Part Number', '').strip(),
			'serial'       : d.get('Serial Number', ''),
			'form_factor'  : d.get('Form Factor', ''),
			'rank'         : d.get('Rank', ''),
			'populated'    : populated,
		})
	return dimms


def collect_system_info():
	'''Collect hostname, kernel, ZFS version, uptime, RAM, CPU, and DIMM info.'''

	info = {
		'hostname'       : capabilities['hostname'],
		'kernel'         : '',
		'zfs_version'    : '',
		'uptime_seconds' : 0,
		'ram_total'      : 0,
		'ram_available'  : 0,
		'cpu_model'      : '',
		'cpu_count'      : os.cpu_count() or 0,
		'dimms'          : [],
	}
	out, _, rc = run_cmd(['uname', '-r'])
	if rc == 0:
		info['kernel'] = out.strip()
	try:
		with open('/sys/module/zfs/version') as f:
			info['zfs_version'] = f.read().strip()
	except FileNotFoundError:
		out, _, rc = run_cmd(['modinfo', '-F', 'version', 'zfs'])
		if rc == 0:
			info['zfs_version'] = out.strip()
	except Exception:
		pass
	try:
		with open('/proc/uptime') as f:
			info['uptime_seconds'] = float(f.read().split()[0])
	except Exception:
		pass
	try:
		with open('/proc/meminfo') as f:
			for line in f:
				if line.startswith('MemTotal:'):
					info['ram_total'] = int(line.split()[1]) * 1024
				elif line.startswith('MemAvailable:'):
					info['ram_available'] = int(line.split()[1]) * 1024
	except Exception:
		pass
	try:
		with open('/proc/cpuinfo') as f:
			for line in f:
				if line.startswith('model name'):
					info['cpu_model'] = line.split(':', 1)[1].strip()
					break
	except Exception:
		pass
	info['dimms'] = collect_dimm_info()
	return info


# ── Health Score ─────────────────────────────────────────────────────────────

def compute_health_score(disk: dict):
	'''
	Compute a 0-100 health score based on SMART attributes, temperature, power-on hours, and defects.

	:param disk: Disk info dict with smart_attributes, temperature, power_on_hours, etc.
	'''

	score = 100.0
	for attr in disk.get('smart_attributes', []):
		raw = attr.get('raw', 0)
		if not isinstance(raw, (int, float)):
			try:
				raw = int(str(raw).replace(',', ''))
			except (ValueError, TypeError):
				raw = 0
		penalty = HEALTH_PENALTIES.get(attr.get('id', 0))
		if penalty and raw > 0:
			score -= min(penalty[0], raw * penalty[1])
		if attr.get('when_failed'):
			score -= 20
	temp = disk.get('temperature')
	if temp and temp > 50:
		score -= (temp - 50) * 2
	poh = disk.get('power_on_hours')
	if poh and poh > 40000:
		score -= min(15, (poh - 40000) // 5000)
	gdc = disk.get('grown_defect_count')
	if isinstance(gdc, (int, float)) and gdc > 0:
		score -= min(30, int(gdc) * 3)
	if disk.get('health') is False:
		score = min(score, 10)
	return max(0, min(100, int(score)))


# ── Fast Temperature ─────────────────────────────────────────────────────────


# ── Disk & SMART Collection ─────────────────────────────────────────────────

def collect_smart(device: str):
	'''
	Run smartctl -j -a on a device and parse the JSON output.

	:param device: Block device path (e.g. /dev/sda)
	'''

	out, _, _ = run_cmd(['smartctl', '-j', '-a', device], timeout=30)
	try:
		data = json.loads(out)
	except (json.JSONDecodeError, ValueError):
		return {'smart_available': False}

	info = {
		'smart_available'    : data.get('smart_support', {}).get('available', False),
		'smart_enabled'      : data.get('smart_support', {}).get('enabled', False),
		'health'             : data.get('smart_status', {}).get('passed'),
		'temperature'        : data.get('temperature', {}).get('current'),
		'power_on_hours'     : data.get('power_on_time', {}).get('hours'),
		'model_family'       : data.get('model_family', ''),
		'firmware'           : data.get('firmware_version', ''),
		'device_model'       : data.get('model_name', ''),
		'user_capacity'      : data.get('user_capacity', {}).get('bytes', 0),
		'rotation_rate'      : data.get('rotation_rate', 0),
		'form_factor'        : data.get('form_factor', {}).get('name', ''),
		'protocol'           : data.get('device', {}).get('protocol', '') or '',
		'sata_version'       : data.get('sata_version', {}).get('string', ''),
		'smart_attributes'   : [],
		'sas_error_counters' : None,
		'grown_defect_count' : None,
	}

	if 'ata_smart_attributes' in data:
		for attr in data['ata_smart_attributes'].get('table', []):
			info['smart_attributes'].append({
				'id'         : attr.get('id', 0),
				'name'       : attr.get('name', ''),
				'value'      : attr.get('value', 0),
				'worst'      : attr.get('worst', 0),
				'threshold'  : attr.get('thresh', 0),
				'raw'        : attr.get('raw', {}).get('value', 0),
				'flags'      : attr.get('flags', {}).get('string', ''),
				'when_failed': attr.get('when_failed', ''),
			})

	if 'scsi_error_counter_log' in data:
		info['sas_error_counters'] = data['scsi_error_counter_log']

	if 'scsi_grown_defect_list' in data:
		info['grown_defect_count'] = data['scsi_grown_defect_list']

	return info


def collect_udev_info(device: str):
	'''
	Query udevadm for device properties as a fallback when smartctl lacks data
	(e.g. USB flash drives that aren't in smartctl's drivedb).

	:param device: Block device path (e.g. /dev/sde)
	'''

	out, _, rc = run_cmd(['udevadm', 'info', '--query=property', f'--name={device}'], timeout=5)
	if rc != 0:
		return {}
	props = {}
	for line in out.splitlines():
		if '=' in line:
			k, v = line.split('=', 1)
			props[k] = v
	info = {}
	vendor = (props.get('ID_VENDOR') or props.get('ID_USB_VENDOR') or '').replace('_', ' ').strip()
	model  = (props.get('ID_MODEL')  or props.get('ID_USB_MODEL')  or '').replace('_', ' ').strip()
	if vendor and model:
		info['model_family'] = f'{vendor} {model}'
	elif vendor:
		info['model_family'] = vendor
	bus = (props.get('ID_BUS') or '').lower()
	if bus:
		info['protocol'] = bus.upper()
	return info


def collect_disks():
	'''Enumerate physical disks via lsblk and collect SMART data in parallel.'''

	out, _, rc = run_cmd(['lsblk', '-d', '-b', '-o', 'NAME,SIZE,MODEL,SERIAL,ROTA,TRAN,TYPE', '-J'])
	if rc != 0:
		return []
	try:
		data = json.loads(out)
	except json.JSONDecodeError:
		return []

	pool_map = collect_pool_mapping()
	devs = []
	for dev in data.get('blockdevices', []):
		if dev.get('type') != 'disk':
			continue
		name = dev['name']
		devs.append({
			'name'       : name,
			'path'       : f'/dev/{name}',
			'size'       : int(dev.get('size') or 0),
			'model'      : (dev.get('model') or '').strip() or 'Unknown',
			'serial'     : (dev.get('serial') or '').strip() or 'Unknown',
			'rotational' : bool(dev.get('rota')),
			'transport'  : dev.get('tran') or 'unknown',
			'pool'       : pool_map.get(name, ''),
		})

	if capabilities['smartctl'] and devs:
		with ThreadPoolExecutor(max_workers=min(8, len(devs))) as executor:
			futures = {executor.submit(collect_smart, d['path']): d for d in devs}
			for future in as_completed(futures):
				disk = futures[future]
				try:
					disk.update(future.result(timeout=45))
				except Exception as e:
					logging.warning('SMART failed for %s: %s', disk['name'], e)

	for d in devs:
		if not d.get('model_family') or not d.get('protocol'):
			udev = collect_udev_info(d['path'])
			if not d.get('model_family') and udev.get('model_family'):
				d['model_family'] = udev['model_family']
			if not d.get('protocol') and udev.get('protocol'):
				d['protocol'] = udev['protocol']
		if not d.get('protocol') and d.get('transport'):
			d['protocol'] = d['transport'].upper()
		d['health_score'] = compute_health_score(d)

	with lock:
		cache['pool_map'] = pool_map

	return devs


# ── ZFS Collection ───────────────────────────────────────────────────────────

def collect_pool_mapping():
	'''Parse zpool status to build a device name to pool name mapping.'''

	if not capabilities['zfs']:
		return {}
	mapping = {}
	out, _, rc = run_cmd(['zpool', 'status', '-L'])
	if rc != 0:
		out, _, rc = run_cmd(['zpool', 'status'])
		if rc != 0:
			return {}
	current_pool = None
	for line in out.splitlines():
		m = re.match(r'\s*pool:\s+(\S+)', line)
		if m:
			current_pool = m.group(1)
			continue
		if current_pool:
			m2 = re.match(r'\s+(/dev/)?(\S+)\s+(ONLINE|DEGRADED|FAULTED|OFFLINE|UNAVAIL|REMOVED)', line)
			if m2:
				dev = m2.group(2)
				if dev == current_pool or dev.endswith(':'):
					continue
				if re.match(r'^(mirror|raidz[123]?|spare|log|cache|special|replacing)(-\d+)?$', dev):
					continue
				dev = os.path.basename(os.path.realpath(f'/dev/{dev}')) if os.path.exists(f'/dev/{dev}') else dev
				dev = re.sub(r'-part\d+$', '', dev)
				dev = re.sub(r'p\d+$', '', dev) if re.match(r'^nvme\d+n\d+p\d+$', dev) else dev
				dev = re.sub(r'\d+$', '', dev) if re.match(r'^sd[a-z]+\d+$', dev) else dev
				mapping[dev] = current_pool
	return mapping


def parse_scrub_age(scan_text: str):
	'''
	Extract the number of days since the last scrub from zpool scan text.

	:param scan_text: The scan line from zpool status output
	'''

	if not scan_text or 'scrub' not in scan_text.lower():
		return None
	m = re.search(r'on\s+\w+\s+(\w+\s+\d+\s+[\d:]+\s+\d{4})', scan_text)
	if not m:
		return None
	try:
		return (datetime.now() - datetime.strptime(m.group(1), '%b %d %H:%M:%S %Y')).days
	except (ValueError, TypeError):
		return None


def collect_pools():
	'''Collect ZFS pool stats, vdev tree, scrub info, and error summary.'''

	if not capabilities['zfs']:
		return []
	out, _, rc = run_cmd(['zpool', 'list', '-Hp', '-o', 'name,size,alloc,free,frag,cap,dedup,health,ashift'])
	if rc != 0:
		return []
	pools = []
	for line in out.strip().splitlines():
		if not line.strip():
			continue
		p = line.split('\t')
		if len(p) < 8:
			continue
		pool = {
			'name'          : p[0],
			'size'          : int(p[1]),
			'allocated'     : int(p[2]),
			'free'          : int(p[3]),
			'fragmentation' : int(p[4]) if p[4] != '-' else 0,
			'capacity_pct'  : int(p[5]) if p[5] != '-' else 0,
			'dedup'         : float(p[6].rstrip('x')) if p[6] != '-' else 1.0,
			'health'        : p[7],
			'ashift'        : int(p[8]) if len(p) > 8 and p[8] != '-' else 0,
			'scan'          : '',
			'vdevs'         : [],
			'errors_summary': '',
			'scrub_age_days': None,
		}
		s_out, _, _ = run_cmd(['zpool', 'status', '-L', pool['name']])
		if s_out:
			pool['scan'], pool['vdevs'], pool['errors_summary'] = parse_pool_status(s_out)
			pool['scrub_age_days'] = parse_scrub_age(pool['scan'])
		pools.append(pool)
	return pools


def parse_pool_status(text: str):
	'''
	Parse raw zpool status output into scan text, vdev list, and error summary.

	:param text: Raw output from zpool status
	'''

	scan_lines = []
	vdevs = []
	errors_summary = ''
	in_scan = False
	in_config = False
	for line in text.splitlines():
		if line.strip().startswith('scan:'):
			in_scan = True
			scan_lines.append(line.split('scan:', 1)[1].strip())
			continue
		if in_scan:
			if line.startswith('\t') and not line.strip().startswith('NAME'):
				scan_lines.append(line.strip())
			else:
				in_scan = False
		if line.strip().startswith('NAME') and 'STATE' in line:
			in_config = True
			continue
		if in_config:
			if not line.strip() or line.strip().startswith('errors:'):
				in_config = False
				if line.strip().startswith('errors:'):
					errors_summary = line.split('errors:', 1)[1].strip()
				continue
			parts = line.split()
			if len(parts) >= 2:
				vdevs.append({
					'name'  : parts[0],
					'state' : parts[1] if len(parts) > 1 else '',
					'read'  : parts[2] if len(parts) > 2 else '0',
					'write' : parts[3] if len(parts) > 3 else '0',
					'cksum' : parts[4] if len(parts) > 4 else '0',
					'indent': len(line) - len(line.lstrip()),
				})
	return ' '.join(scan_lines), vdevs, errors_summary


def collect_datasets_and_snapshots():
	'''Collect all ZFS datasets and snapshots in a single zfs list call.'''

	if not capabilities['zfs']:
		return [], []
	out, _, rc = run_cmd(['zfs', 'list', '-t', 'all', '-Hp', '-o', 'name,used,avail,refer,mountpoint,compression,compressratio,recordsize,type,quota,reservation,creation', '-s', 'creation'])
	if rc != 0:
		return [], []
	datasets = []
	snapshots = []
	for line in out.strip().splitlines():
		if not line.strip():
			continue
		p = line.split('\t')
		if len(p) < 9:
			continue
		if p[8] == 'snapshot':
			try:
				creation = int(p[11]) if len(p) > 11 else 0
			except (ValueError, TypeError):
				creation = p[11] if len(p) > 11 else 0
			snapshots.append({
				'name'       : p[0],
				'used'       : int(p[1]) if p[1] != '-' else 0,
				'referenced' : int(p[3]) if p[3] != '-' else 0,
				'creation'   : creation,
			})
		else:
			datasets.append({
				'name'          : p[0],
				'used'          : int(p[1]) if p[1] != '-' else 0,
				'available'     : int(p[2]) if p[2] != '-' else 0,
				'referenced'    : int(p[3]) if p[3] != '-' else 0,
				'mountpoint'    : p[4] if p[4] != '-' else '',
				'compression'   : p[5] if p[5] != '-' else 'off',
				'compressratio' : p[6] if p[6] != '-' else '1.00x',
				'recordsize'    : int(p[7]) if p[7] not in ('-', '') else 0,
				'type'          : p[8],
				'quota'         : int(p[9]) if len(p) > 9 and p[9] not in ('-', '0', 'none', '') else 0,
				'reservation'   : int(p[10]) if len(p) > 10 and p[10] not in ('-', '0', 'none', '') else 0,
			})
	return datasets, snapshots


# ── I/O Collection ───────────────────────────────────────────────────────────

def collect_iostat():
	'''Read /proc/diskstats and compute per-disk I/O rates from deltas.'''

	global _prev_diskstats
	current = {}
	now = time.time()
	try:
		with open('/proc/diskstats') as f:
			for line in f:
				parts = line.split()
				if len(parts) < 14:
					continue
				name = parts[2]
				if not re.match(r'^(sd[a-z]+|nvme\d+n\d+|dm-\d+|vd[a-z]+|xvd[a-z]+)$', name):
					continue
				current[name] = {
					'read_ios'      : int(parts[3]),
					'read_sectors'  : int(parts[5]),
					'read_ticks'    : int(parts[6]),
					'write_ios'     : int(parts[7]),
					'write_sectors' : int(parts[9]),
					'write_ticks'   : int(parts[10]),
					'io_ticks'      : int(parts[12]) if len(parts) > 12 else 0,
					'ts'            : now,
				}
	except FileNotFoundError:
		return {}

	rates = {}
	if _prev_diskstats:
		for name, cur in current.items():
			prev = _prev_diskstats.get(name)
			if not prev:
				continue
			dt = cur['ts'] - prev['ts']
			if dt <= 0:
				continue
			d_rio = cur['read_ios'] - prev['read_ios']
			d_wio = cur['write_ios'] - prev['write_ios']
			rates[name] = {
				'read_bps'     : (cur['read_sectors'] - prev['read_sectors']) * 512 / dt,
				'write_bps'    : (cur['write_sectors'] - prev['write_sectors']) * 512 / dt,
				'read_iops'    : d_rio / dt,
				'write_iops'   : d_wio / dt,
				'read_lat_ms'  : (cur['read_ticks'] - prev['read_ticks']) / d_rio if d_rio > 0 else 0,
				'write_lat_ms' : (cur['write_ticks'] - prev['write_ticks']) / d_wio if d_wio > 0 else 0,
				'busy_pct'     : min(100, (cur['io_ticks'] - prev['io_ticks']) / (dt * 10)),
			}
	_prev_diskstats = current
	return rates



# ── Background Worker ────────────────────────────────────────────────────────

def background_worker():
	'''Collect all monitoring data on timed intervals and update the shared cache.'''

	tick = 0
	collect_iostat()
	time.sleep(1)

	while True:
		try:
			io_rates = collect_iostat()

			with lock:
				cache['io_rates'] = io_rates

			if tick % (POOL_INTERVAL // IO_INTERVAL) == 0:
				pools     = collect_pools()
				datasets, snapshots = collect_datasets_and_snapshots()
				sys_info  = collect_system_info()
				with lock:
					cache['pools']       = pools
					cache['datasets']    = datasets
					cache['snapshots']   = snapshots
					cache['system_info'] = sys_info

			if tick % (SMART_INTERVAL // IO_INTERVAL) == 0:
				disks = collect_disks()
				with lock:
					cache['disks'] = disks

			if not init_done.is_set():
				init_done.set()

			tick += 1
		except Exception:
			logging.exception('Worker error')
		time.sleep(IO_INTERVAL)


# ── WebSocket Client ─────────────────────────────────────────────────────────

async def ws_sender(ws):
	'''
	Stream cache data to the dashboard over WebSocket.

	:param ws: Active WebSocket connection to the dashboard
	'''

	with lock:
		io_msg       = json.dumps({'type': 'io',        'ts': time.time(), 'rates': cache['io_rates'], 'pool_map': cache['pool_map']})
		pools_msg    = json.dumps({'type': 'pools',     'ts': time.time(), 'pools': cache['pools']})
		datasets_msg = json.dumps({'type': 'datasets',  'ts': time.time(), 'datasets': cache['datasets']})
		snaps_msg    = json.dumps({'type': 'snapshots', 'ts': time.time(), 'snapshots': cache['snapshots']})
		disks_msg    = json.dumps({'type': 'disks',     'ts': time.time(), 'disks': cache['disks']})
		system_msg   = json.dumps({'type': 'system',    'ts': time.time(), 'info': cache['system_info']})

	await ws.send(system_msg)
	await ws.send(pools_msg)
	await ws.send(datasets_msg)
	await ws.send(snaps_msg)
	await ws.send(disks_msg)
	await ws.send(io_msg)

	tick = 0
	while True:
		await asyncio.sleep(IO_INTERVAL)
		tick += 1

		with lock:
			io_msg = json.dumps({'type': 'io', 'ts': time.time(), 'rates': cache['io_rates'], 'pool_map': cache['pool_map']})
		await ws.send(io_msg)

		if tick % (POOL_INTERVAL // IO_INTERVAL) == 0:
			with lock:
				pools_msg    = json.dumps({'type': 'pools',     'ts': time.time(), 'pools': cache['pools']})
				datasets_msg = json.dumps({'type': 'datasets',  'ts': time.time(), 'datasets': cache['datasets']})
				snaps_msg    = json.dumps({'type': 'snapshots', 'ts': time.time(), 'snapshots': cache['snapshots']})
				system_msg   = json.dumps({'type': 'system',    'ts': time.time(), 'info': cache['system_info']})
			await ws.send(pools_msg)
			await ws.send(datasets_msg)
			await ws.send(snaps_msg)
			await ws.send(system_msg)

		if tick % (SMART_INTERVAL // IO_INTERVAL) == 0:
			with lock:
				disks_msg = json.dumps({'type': 'disks', 'ts': time.time(), 'disks': cache['disks']})
			await ws.send(disks_msg)


async def ws_receiver(ws):
	'''
	Receive and execute commands from the dashboard.

	:param ws: Active WebSocket connection to the dashboard
	'''

	async for raw in ws:
		try:
			data = json.loads(raw)
		except json.JSONDecodeError:
			continue
		cmd = data.get('type')
		if cmd == 'smarttest':
			device    = data.get('device', '')
			test_type = data.get('test_type', 'short')
			if test_type not in ('short', 'long', 'conveyance'):
				continue
			if not re.match(r'^/dev/(sd[a-z]+|nvme\d+n\d+|da\d+)$', device):
				continue
			out, err, rc = await asyncio.to_thread(run_cmd, ['smartctl', '-t', test_type, device])
			await ws.send(json.dumps({
				'type': 'smarttest_result', 'device': device,
				'test_type': test_type, 'success': rc == 0,
				'output': out.strip(),
			}))


async def ws_main(dashboard_url: str):
	'''
	Connect to the dashboard and maintain the WebSocket link with auto-reconnect.

	:param dashboard_url: WebSocket URL of the dashboard (e.g. ws://10.0.0.50:8888/ws/agent)
	'''

	while True:
		try:
			async with websockets.connect(dashboard_url, ping_interval=20, ping_timeout=10, max_size=2**22, close_timeout=5) as ws:
				logging.info('Connected to dashboard at %s', dashboard_url)
				await ws.send(json.dumps({
					'type': 'hello',
					'hostname': capabilities['hostname'],
					'capabilities': capabilities,
				}))
				await asyncio.gather(ws_sender(ws), ws_receiver(ws))
		except Exception as e:
			logging.warning('WebSocket (%s: %s), reconnecting in 5s...', type(e).__name__, e)
		await asyncio.sleep(5)



if __name__ == '__main__':
	# Parse command line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('dashboard_url', help='Dashboard WebSocket URL (e.g. ws://10.0.0.50:8888/ws/agent)')
	parser.add_argument('-d', '--debug', action='store_true', help='Enable debug logging')
	args = parser.parse_args()

	# Setup logging
	if args.debug:
		apv.setup_logging(level='DEBUG', log_to_disk=True, max_log_size=5*1024*1024, max_backups=5, compress_backups=True, log_file_name='havoc', show_details=True)
		logging.debug('Debug logging enabled')
	else:
		apv.setup_logging(level='INFO')

	detect_capabilities()

	if os.geteuid() != 0:
		raise RuntimeError('This program must be ran as root')

	logging.info('ZPulse Agent starting — host: %s', capabilities['hostname'])
	logging.info('  smartctl:  %s', 'available' if capabilities['smartctl'] else 'NOT FOUND')
	logging.info('  zfs:       %s', 'available' if capabilities['zfs']      else 'NOT FOUND')
	logging.info('  dashboard: %s', args.dashboard_url)

	worker = threading.Thread(target=background_worker, daemon=True)
	worker.start()
	init_done.wait(timeout=120)

	asyncio.run(ws_main(args.dashboard_url))