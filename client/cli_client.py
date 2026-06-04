#!/usr/bin/env python3
"""
cli_client.py – Terminal client for the real-time orders system.

Connects to the STOMP WebSocket endpoint and prints incoming order
events to stdout as they arrive.  Also lets you create, update, and
delete orders without opening a browser.

Requirements:
    pip install websocket-client stomp.py requests

Usage:
    python cli_client.py [--host localhost] [--port 8080]
"""

import argparse
import json
import sys
import threading
import time
from datetime import datetime

try:
    import requests
    import stomp
except ImportError:
    print("[ERROR] Missing dependencies. Run:  pip install stomp.py requests")
    sys.exit(1)

# ANSI colours
RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
BLUE   = "\033[94m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
GREY   = "\033[90m"


def colour_op(op: str) -> str:
    colours = {"INSERT": GREEN, "UPDATE": BLUE, "DELETE": RED}
    return f"{colours.get(op, RESET)}{BOLD}{op}{RESET}"


def colour_status(status: str) -> str:
    colours = {"pending": YELLOW, "shipped": BLUE, "delivered": GREEN}
    return f"{colours.get(status, RESET)}{status}{RESET}"


class OrderListener(stomp.ConnectionListener):
    """STOMP listener that prints order change events."""

    def on_connected(self, frame):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"{GREY}[{ts}]{RESET} {GREEN}{BOLD}✓ Connected to WebSocket{RESET}")

    def on_disconnected(self):
        print(f"{RED}{BOLD}✗ Disconnected from WebSocket{RESET}")

    def on_message(self, frame):
        ts = datetime.now().strftime("%H:%M:%S")
        try:
            event = json.loads(frame.body)
            op    = event.get("operation", "?")
            oid   = event.get("id", "?")

            if op == "DELETE":
                print(f"{GREY}[{ts}]{RESET}  {colour_op(op)}  order #{oid}  {RED}(removed){RESET}")
            else:
                customer = event.get("customerName", "?")
                product  = event.get("productName",  "?")
                status   = event.get("status",       "?")
                print(
                    f"{GREY}[{ts}]{RESET}  {colour_op(op)}"
                    f"  #{oid}"
                    f"  {BOLD}{customer}{RESET}"
                    f"  ·  {CYAN}{product}{RESET}"
                    f"  →  {colour_status(status)}"
                )
        except Exception as ex:
            print(f"{RED}[ERROR] Could not parse event: {ex}{RESET}")

    def on_error(self, frame):
        print(f"{RED}[STOMP ERROR]{RESET} {frame.body}")


def make_base_url(host: str, port: int) -> str:
    return f"http://{host}:{port}/api/orders"


def list_orders(base: str):
    resp = requests.get(base, timeout=5)
    resp.raise_for_status()
    orders = resp.json()
    if not orders:
        print("  (no orders)")
        return
    print(f"\n  {'ID':<5} {'Customer':<20} {'Product':<22} {'Status':<12} Updated")
    print("  " + "─" * 75)
    for o in orders:
        print(
            f"  {o['id']:<5} {o['customerName']:<20} {o['productName']:<22}"
            f" {colour_status(o['status']):<20} {o.get('updatedAt','?')}"
        )
    print()


def create_order(base: str, customer: str, product: str, status: str = "pending"):
    payload = {"customerName": customer, "productName": product, "status": status}
    resp = requests.post(base, json=payload, timeout=5)
    resp.raise_for_status()
    o = resp.json()
    print(f"  {GREEN}✓ Created order #{o['id']}{RESET}")


def update_order(base: str, oid: int, status: str):
    resp = requests.patch(f"{base}/{oid}", json={"status": status}, timeout=5)
    resp.raise_for_status()
    print(f"  {BLUE}✓ Updated order #{oid} → {colour_status(status)}{RESET}")


def delete_order(base: str, oid: int):
    resp = requests.delete(f"{base}/{oid}", timeout=5)
    resp.raise_for_status()
    print(f"  {RED}✓ Deleted order #{oid}{RESET}")


def print_help():
    print(f"""
  {BOLD}Commands:{RESET}
    list                          – show all current orders
    add <customer> <product>      – create a pending order
    status <id> <status>          – update status (pending/shipped/delivered)
    delete <id>                   – delete an order
    help                          – show this message
    exit / quit                   – disconnect and exit
""")


def interactive_loop(base: str):
    print_help()
    while True:
        try:
            raw = input(f"{CYAN}orders>{RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
            break

        if not raw:
            continue

        parts = raw.split(None, 3)
        cmd   = parts[0].lower()

        try:
            if cmd == "list":
                list_orders(base)

            elif cmd == "add" and len(parts) >= 3:
                create_order(base, parts[1], parts[2])

            elif cmd == "status" and len(parts) >= 3:
                update_order(base, int(parts[1]), parts[2])

            elif cmd == "delete" and len(parts) >= 2:
                delete_order(base, int(parts[1]))

            elif cmd in ("exit", "quit"):
                print("Bye!")
                break

            elif cmd == "help":
                print_help()

            else:
                print(f"  {YELLOW}Unknown command. Type 'help' for usage.{RESET}")

        except requests.HTTPError as e:
            print(f"  {RED}HTTP error: {e}{RESET}")
        except ValueError:
            print(f"  {RED}Invalid argument.{RESET}")
        except Exception as e:
            print(f"  {RED}Error: {e}{RESET}")


def main():
    parser = argparse.ArgumentParser(description="Real-time orders CLI client")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default=8080, type=int)
    args = parser.parse_args()

    base = make_base_url(args.host, args.port)

    print(f"\n{BOLD}Real-Time Orders CLI{RESET}")
    print(f"Connecting to  ws://{args.host}:{args.port}/ws\n")

    # Set up STOMP over WebSocket
    # stomp.py uses raw WebSocket; the server exposes the SockJS raw WS endpoint.
    conn = stomp.Connection(
        host_and_ports=[(args.host, args.port)],
        use_ssl=False,
    )
    conn.set_listener("", OrderListener())

    try:
        conn.connect(wait=True, headers={"heart-beat": "0,0"})
        # Subscribe to the order changes topic
        conn.subscribe(destination="/topic/orders", id=1, ack="auto")
    except Exception as ex:
        print(f"{RED}Could not connect: {ex}{RESET}")
        print("Make sure the Spring Boot server is running.")
        sys.exit(1)

    print(f"{GREY}Listening for live events in the background…{RESET}")
    print(f"{GREY}Events will appear above the prompt as they arrive.{RESET}\n")

    try:
        interactive_loop(base)
    finally:
        conn.disconnect()


if __name__ == "__main__":
    main()
