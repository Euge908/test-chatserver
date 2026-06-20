#!/usr/bin/env python3
"""
WebSocket load generator.
Spawns N clients that each continuously send  <client_id>_<seq>  on an interval.

Usage:
    python load_gen.py                        # 3 clients, 1 msg/sec
    python load_gen.py --clients 5 --interval 0.5
    python load_gen.py --url ws://host:8000/ws
"""
import argparse
import asyncio

import websockets

DEFAULT_URL = "ws://localhost:8000/ws/{client_id}"


async def run_client(client_id: int, interval: float, url_template: str) -> None:
    uri = url_template.format(client_id=client_id)
    seq = 0
    while True:
        try:
            async with websockets.connect(uri) as ws:
                print(f"[client {client_id:>3}] connected")

                async def drain() -> None:
                    try:
                        async for _ in ws:
                            pass  # discard incoming — we're just a sender
                    except Exception:
                        pass

                drain_task = asyncio.create_task(drain())
                try:
                    while True:
                        seq += 1
                        msg = f"{client_id}_{seq}"
                        await ws.send(msg)
                        print(f"[client {client_id:>3}] → {msg}")
                        await asyncio.sleep(interval)
                finally:
                    drain_task.cancel()
                    try:
                        await drain_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            print(f"[client {client_id:>3}] disconnected ({e}), retrying in 2s…")
            await asyncio.sleep(2)


async def main(n_clients: int, interval: float, url_template: str) -> None:
    print(f"Starting {n_clients} client(s), {interval}s interval. Ctrl+C to stop.\n")
    await asyncio.gather(*[
        asyncio.create_task(run_client(i + 1, interval, url_template))
        for i in range(n_clients)
    ])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebSocket load generator")
    parser.add_argument("--clients",  type=int,   default=3,
                        help="number of concurrent clients (default: 3)")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between sends per client (default: 1.0)")
    parser.add_argument("--url",      type=str,   default=DEFAULT_URL,
                        help=f"URL template, use {{client_id}} as placeholder (default: {DEFAULT_URL})")
    args = parser.parse_args()
    try:
        asyncio.run(main(args.clients, args.interval, args.url))
    except KeyboardInterrupt:
        print("\nStopped.")
