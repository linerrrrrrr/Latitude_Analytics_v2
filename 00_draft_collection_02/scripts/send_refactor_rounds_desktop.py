"""Send a finite prepared prompt queue serially through the existing desktop task."""
import argparse
import json
import os
from pathlib import Path
import queue
import subprocess
import threading
import time

from continue_refactor_rounds import THREAD_ID, publish_status


class DesktopMcp:
    def __init__(self, log_dir):
        self.next_id = 0
        self.incoming = queue.Queue()
        self.error_stream = (log_dir / 'mcp.stderr.log').open('w', encoding='utf-8')
        self.process = subprocess.Popen([
            os.environ['CODEX_MCP_NODE_PATH'],
            str(Path.home() / '.codex/plugins/cache/openai-bundled/codex-app-tools/0.1.5/server.mjs'),
        ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.error_stream,
            text=True, encoding='utf-8', bufsize=1)
        self.reader = threading.Thread(target=self.read_messages, daemon=True)
        self.reader.start()
        self.call('initialize', {'protocolVersion': '2024-11-05', 'capabilities': {},
                  'clientInfo': {'name': 'authorized-refactor-batch', 'version': '1.0'}})
        self.process.stdin.write(json.dumps({'jsonrpc': '2.0', 'method': 'notifications/initialized'})+'\n')
        self.process.stdin.flush()

    def read_messages(self):
        for line in self.process.stdout:
            try:
                self.incoming.put(json.loads(line))
            except ValueError as error:
                self.incoming.put({'error': {'message': str(error)}})
        self.incoming.put({'error': {'message': 'MCP transport closed'}})

    def call(self, method, params, heartbeat=None):
        self.next_id += 1
        request_id = self.next_id
        self.process.stdin.write(json.dumps({'jsonrpc': '2.0', 'id': request_id,
                                            'method': method, 'params': params})+'\n')
        self.process.stdin.flush()
        deadline = time.monotonic() + 90
        while True:
            if heartbeat:
                heartbeat()
            if time.monotonic() > deadline:
                raise RuntimeError('MCP request timeout; no resend')
            try:
                response = self.incoming.get(timeout=1)
            except queue.Empty:
                continue
            if response.get('error'):
                raise RuntimeError(str(response['error']))
            if response.get('id') == request_id:
                return response['result']

    def tool(self, name, arguments, heartbeat=None):
        result = self.call('tools/call', {'name': name, 'arguments': arguments,
                           '_meta': {'threadId': os.environ['CODEX_THREAD_ID']}}, heartbeat)
        if result.get('isError'):
            raise RuntimeError(str(result))
        return json.loads('\n'.join(item['text'] for item in result['content'] if item['type']=='text'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', required=True, type=Path)
    parser.add_argument('--probe', action='store_true')
    parser.add_argument('--queue', required=True, type=Path)
    parser.add_argument('--expected-turn', required=True)
    args = parser.parse_args()
    project_markers = ['.git', '.env', 'config/settings.py']
    current_path = Path.cwd().resolve()
    for candidate_root in [current_path, *current_path.parents]:
        if all((candidate_root / marker).exists() for marker in project_markers):
            break
    else:
        raise RuntimeError('未找到项目根目录')
    run_dir = args.run_dir.resolve()
    if not run_dir.is_relative_to(candidate_root / '00_draft_collection_02/run_status'):
        raise ValueError('Run evidence must be inside draft run_status')
    run_dir.mkdir(parents=True, exist_ok=True)
    prompt_queue = json.loads(args.queue.read_text(encoding='utf-8'))
    if not prompt_queue or [item['round'] for item in prompt_queue] != list(range(1, len(prompt_queue)+1)):
        raise ValueError('Queue must be finite, nonempty and consecutively numbered')
    if any(not item.get('prompt', '').strip() for item in prompt_queue):
        raise ValueError('Empty prompt in queue')
    client = DesktopMcp(run_dir)
    if args.probe:
        try:
            print(json.dumps(client.tool('wait_threads', {'targets': [{'threadId': THREAD_ID}],
                             'timeoutMs': 0}), ensure_ascii=False))
        finally:
            client.process.terminate()
        return
    with (run_dir/'started.lock').open('x', encoding='utf-8') as stream:
        stream.write(str(os.getpid()))
    status_path = run_dir/'status.json'
    status = dict(state='starting', worker_pid=os.getpid(), child_pid=client.process.pid,
                  round=0, completed_rounds=0, total_rounds=len(prompt_queue), phase='desktop_monitor',
                  started_at=time.time(), last_event='', error='', thread_id=THREAD_ID)
    monitor = None
    try:
        publish_status(status_path, status)
        monitor = subprocess.Popen(['powershell.exe', '-NoProfile', '-ExecutionPolicy', 'Bypass',
                 '-File', str(Path(__file__).with_name('monitor_refactor_rounds.ps1')),
                 '-RunDir', str(run_dir)], creationflags=subprocess.CREATE_NEW_CONSOLE)
        status['monitor_pid'] = monitor.pid
        monitor_heartbeat = run_dir/'monitor.heartbeat'
        ready_deadline = time.monotonic()+20
        while not monitor_heartbeat.exists():
            if monitor.poll() is not None or time.monotonic()>ready_deadline:
                raise RuntimeError('Monitor failed to start')
            time.sleep(.5)

        def heartbeat():
            if monitor.poll() is not None or time.time()-monitor_heartbeat.stat().st_mtime>20:
                raise RuntimeError('Monitor stopped; no further prompts will be sent')
            if (run_dir/'STOP').exists():
                raise RuntimeError('STOP requested; no further prompts will be sent')
            publish_status(status_path, status)

        snapshot = client.tool('wait_threads', {'targets': [{'threadId': THREAD_ID}], 'timeoutMs': 0}, heartbeat)
        initial_poll = snapshot['polls'][0]
        if initial_poll['thread']['status']['type'] != 'idle':
            raise RuntimeError('Target is not idle; refusing to send into an active turn')
        previous_turn_id = initial_poll['latestTurn']['id']
        if previous_turn_id != args.expected_turn:
            raise RuntimeError('Target advanced since queue preparation; do not send stale prompts')
        for entry in prompt_queue:
            number, name, prompt = entry['round'], entry['name'], entry['prompt']
            status.update(state='running', round=number, phase=name, target=entry.get('target', ''), script_round=entry.get('script_round'), last_event='sending one prepared prompt')
            heartbeat()
            (run_dir/f'{number:02d}-{name}.prompt.txt').write_text(prompt, encoding='utf-8')
            sent = client.tool('send_message_to_thread', {'threadId': THREAD_ID, 'prompt': prompt}, heartbeat)
            (run_dir/f'{number:02d}-{name}.sent.json').write_text(json.dumps(sent), encoding='utf-8')
            round_deadline = time.monotonic()+45*60
            expected_turn_id = None
            cursor = None
            while True:
                if time.monotonic()>round_deadline:
                    raise RuntimeError('Round exceeded 45 minutes; stopped advancing')
                target = {'threadId': THREAD_ID}
                if cursor:
                    target['afterCursor'] = cursor
                snapshot = client.tool('wait_threads', {'targets': [target], 'timeoutMs': 10000}, heartbeat)
                if snapshot.get('errors'):
                    raise RuntimeError(str(snapshot['errors']))
                poll = snapshot['polls'][0]
                cursor = poll.get('cursor')
                (run_dir/f'{number:02d}-{name}.latest.json').write_text(json.dumps(snapshot, ensure_ascii=False), encoding='utf-8')
                turn = poll.get('latestTurn') or {}
                if turn.get('id') == previous_turn_id:
                    time.sleep(1)
                    continue
                if not turn.get('id'):
                    raise RuntimeError('Missing current turn identity')
                if expected_turn_id is None:
                    expected_turn_id = turn['id']
                elif expected_turn_id != turn['id']:
                    raise RuntimeError('Another turn intervened; stop to avoid duplicate work')
                status['last_event'] = turn.get('status', '')
                status['turn_id'] = expected_turn_id
                heartbeat()
                if turn.get('error') or turn.get('status') in ('failed', 'interrupted'):
                    raise RuntimeError(f'Target turn failed: {turn}')
                if turn.get('status') == 'completed':
                    if poll['thread']['status']['type'] != 'idle':
                        raise RuntimeError('Turn completed but target is not idle')
                    break
                if poll['thread']['status'].get('activeFlags'):
                    raise RuntimeError('Target needs attention; stopped advancing')
            status.update(completed_rounds=number, summary=f'Round {number} finished; no business checks added by sender')
            previous_turn_id = expected_turn_id
            heartbeat()
        status.update(state='completed', phase='batch_complete')
        publish_status(status_path, status)
    except BaseException as error:
        status.update(state='failed', error=str(error))
        publish_status(status_path, status)
        raise
    finally:
        client.process.terminate()
        client.process.wait(timeout=10)
        client.error_stream.close()


if __name__ == '__main__':
    main()
