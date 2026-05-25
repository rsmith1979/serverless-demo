# Module 9: Async vs Sync Comparison Script
# ==========================================
# Replaces generator.py with a clear side-by-side comparison of
# synchronous vs asynchronous response times and behavior.
#
# Prerequisites:
#   - Module 9 SAM stack deployed (workshop-cost-tuning)
#   - Python 3.9+ with boto3, aiohttp installed (same deps as generator.py)
#
# Usage:
#   python3 async_comparison.py -s        # Compare sync vs async (default 10 requests)
#   python3 async_comparison.py -s -n 20  # 20 requests each
#   python3 async_comparison.py -g        # Generate heavy traffic (1000 requests)

import sys
import getopt
import boto3
import asyncio
import aiohttp
from aiohttp import ClientSession
import time
import statistics

STACK_NAME = 'workshop-cost-tuning'
TRANSACTION_POLLING_TIME = 5
DEFAULT_REQUEST_COUNT = 10
TRAFFIC_GENERATION_COUNT = 1000


def get_urls_from_cfn():
    """Retrieve API Gateway endpoints from CloudFormation stack outputs."""
    cloudformation = boto3.resource('cloudformation')
    stack = cloudformation.Stack(STACK_NAME)
    urls = {}
    for output in stack.outputs:
        urls[output['OutputKey']] = output['OutputValue']
    return urls


async def timed_request(url, session):
    """Make a GET request and return elapsed time in ms + response body."""
    start = time.perf_counter()
    try:
        resp = await session.request(method="GET", url=url)
        body = await resp.text()
        elapsed = (time.perf_counter() - start) * 1000
        return {'elapsed_ms': round(elapsed, 1), 'status': resp.status, 'body': body}
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return {'elapsed_ms': round(elapsed, 1), 'status': 'ERROR', 'body': str(e)}


async def run_comparison(urls, num_requests):
    """Send requests to both sync and async endpoints and compare."""
    orders_base = urls['OrdersEndpoint']
    processing_base = urls['ProcessingEndpoint']

    sync_url = f"{orders_base}order-sync"
    async_url = f"{orders_base}order-async"

    print("\n" + "=" * 64)
    print("  MODULE 9: SYNCHRONOUS vs ASYNCHRONOUS COMPARISON")
    print("=" * 64)

    async with ClientSession() as session:

        # --- Phase 1: Synchronous requests ---
        print(f"\n{'='*64}")
        print(f"  PHASE 1: SYNCHRONOUS PATTERN")
        print(f"  Endpoint: {sync_url}")
        print(f"  Sending {num_requests} requests (client waits for full processing)")
        print(f"{'='*64}\n")

        sync_results = []
        for i in range(num_requests):
            result = await timed_request(sync_url, session)
            sync_results.append(result)
            status_icon = "\u2713" if result['status'] == 200 else "\u2717"
            # Extract transaction ID if available
            tx_id = ""
            try:
                import json
                body = json.loads(result['body'])
                tx_id = f" | txn: {body.get('transactionId', 'N/A')[:12]}..."
            except:
                pass
            print(f"  [{status_icon}] Request {i+1:>3}: {result['elapsed_ms']:>8.1f} ms (HTTP {result['status']}){tx_id}")

        # --- Phase 2: Asynchronous requests (initial call only) ---
        print(f"\n{'='*64}")
        print(f"  PHASE 2: ASYNCHRONOUS PATTERN (initial response only)")
        print(f"  Endpoint: {async_url}")
        print(f"  Sending {num_requests} requests (client gets job ID immediately)")
        print(f"{'='*64}\n")

        async_results = []
        job_ids = []
        for i in range(num_requests):
            result = await timed_request(async_url, session)
            async_results.append(result)
            status_icon = "\u2713" if result['status'] == 200 else "\u2717"
            # Extract job ID
            job_id = ""
            try:
                import json
                body = json.loads(result['body'])
                jid = body.get('jobId', '')
                if jid:
                    job_ids.append(jid)
                    job_id = f" | jobId: {jid[:12]}..."
            except:
                pass
            print(f"  [{status_icon}] Request {i+1:>3}: {result['elapsed_ms']:>8.1f} ms (HTTP {result['status']}){job_id}")

        # --- Phase 3: Poll for async results (demonstrate the pattern) ---
        poll_count = min(3, len(job_ids))
        if poll_count > 0:
            print(f"\n{'='*64}")
            print(f"  PHASE 3: POLLING FOR ASYNC RESULTS")
            print(f"  Demonstrating client polling pattern ({poll_count} jobs)")
            print(f"{'='*64}\n")

            for jid in job_ids[:poll_count]:
                poll_url = f"{processing_base}get-transaction/{jid}"
                attempts = 0
                resolved = False
                poll_start = time.perf_counter()

                while not resolved and attempts < 6:
                    attempts += 1
                    result = await timed_request(poll_url, session)
                    try:
                        body = json.loads(result['body'])
                        if 'transactionId' in body:
                            total_poll_time = (time.perf_counter() - poll_start) * 1000
                            print(f"  [\u2713] Job {jid[:12]}... resolved after {attempts} poll(s) ({total_poll_time:.0f} ms total)")
                            print(f"       Transaction ID: {body['transactionId']}")
                            resolved = True
                        else:
                            print(f"  [~] Job {jid[:12]}... poll #{attempts}: not yet complete, waiting {TRANSACTION_POLLING_TIME}s...")
                            await asyncio.sleep(TRANSACTION_POLLING_TIME)
                    except:
                        print(f"  [~] Job {jid[:12]}... poll #{attempts}: waiting {TRANSACTION_POLLING_TIME}s...")
                        await asyncio.sleep(TRANSACTION_POLLING_TIME)

                if not resolved:
                    print(f"  [!] Job {jid[:12]}... not resolved after {attempts} attempts")

    # --- Results Summary ---
    sync_times = [r['elapsed_ms'] for r in sync_results if r['status'] == 200]
    async_times = [r['elapsed_ms'] for r in async_results if r['status'] == 200]

    print(f"\n{'='*64}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*64}")

    if sync_times:
        print(f"\n  SYNCHRONOUS ({len(sync_times)} successful requests)")
        print(f"  {'Metric':<25} {'Value':>10}")
        print(f"  {'-'*25} {'-'*10}")
        print(f"  {'Avg response time':<25} {statistics.mean(sync_times):>8.1f} ms")
        print(f"  {'Median':<25} {statistics.median(sync_times):>8.1f} ms")
        print(f"  {'Min':<25} {min(sync_times):>8.1f} ms")
        print(f"  {'Max':<25} {max(sync_times):>8.1f} ms")
        if len(sync_times) > 1:
            print(f"  {'Std deviation':<25} {statistics.stdev(sync_times):>8.1f} ms")

    if async_times:
        print(f"\n  ASYNCHRONOUS - Initial Response ({len(async_times)} successful requests)")
        print(f"  {'Metric':<25} {'Value':>10}")
        print(f"  {'-'*25} {'-'*10}")
        print(f"  {'Avg response time':<25} {statistics.mean(async_times):>8.1f} ms")
        print(f"  {'Median':<25} {statistics.median(async_times):>8.1f} ms")
        print(f"  {'Min':<25} {min(async_times):>8.1f} ms")
        print(f"  {'Max':<25} {max(async_times):>8.1f} ms")
        if len(async_times) > 1:
            print(f"  {'Std deviation':<25} {statistics.stdev(async_times):>8.1f} ms")

    if sync_times and async_times:
        sync_avg = statistics.mean(sync_times)
        async_avg = statistics.mean(async_times)
        speedup = sync_avg / async_avg if async_avg > 0 else 0
        savings = sync_avg - async_avg

        print(f"\n  COMPARISON")
        print(f"  {'-'*45}")
        print(f"  {'Sync avg (client waits)':<30} {sync_avg:>8.1f} ms")
        print(f"  {'Async avg (immediate response)':<30} {async_avg:>8.1f} ms")
        print(f"  {'Time saved per request':<30} {savings:>8.1f} ms")
        print(f"  {'Client-perceived speedup':<30} {speedup:>8.1f}x faster")
        print(f"  {'Latency reduction':<30} {(savings/sync_avg)*100:>7.1f}%")

    print(f"\n  KEY TAKEAWAY")
    print(f"  {'-'*45}")
    print(f"  Sync:  Client blocked for {statistics.mean(sync_times):.0f} ms on average")
    print(f"  Async: Client free after {statistics.mean(async_times):.0f} ms (gets job ID)")
    print(f"         Processing completes in background")
    print(f"         Client polls when ready for result")
    print(f"\n  Healthcare Example:")
    print(f"  A clinician submits a study for AI analysis.")
    print(f"  Sync:  Clinician stares at loading screen for {statistics.mean(sync_times):.0f}+ ms")
    print(f"  Async: Clinician gets confirmation in {statistics.mean(async_times):.0f} ms,")
    print(f"         moves to next patient, checks results later.")
    print(f"\n{'='*64}\n")


async def generate_traffic(urls):
    """Generate heavy traffic to both endpoints (same as generator.py -g)."""
    orders_base = urls['OrdersEndpoint']
    processing_base = urls['ProcessingEndpoint']
    sync_url = f"{orders_base}order-sync"
    async_url = f"{orders_base}order-async"

    print(f"\n  Generating {TRAFFIC_GENERATION_COUNT} requests to each endpoint...")
    print(f"  This will take a while. Watch CloudWatch metrics for patterns.\n")

    async with ClientSession() as session:
        sync_tasks = [timed_request(sync_url, session) for _ in range(TRAFFIC_GENERATION_COUNT)]
        async_tasks = [timed_request(async_url, session) for _ in range(TRAFFIC_GENERATION_COUNT)]

        all_results = await asyncio.gather(*sync_tasks, *async_tasks, return_exceptions=True)

        sync_count = sum(1 for r in all_results[:TRAFFIC_GENERATION_COUNT] if isinstance(r, dict) and r.get('status') == 200)
        async_count = sum(1 for r in all_results[TRAFFIC_GENERATION_COUNT:] if isinstance(r, dict) and r.get('status') == 200)

        print(f"  Traffic generation complete:")
        print(f"    Sync requests:  {sync_count}/{TRAFFIC_GENERATION_COUNT} successful")
        print(f"    Async requests: {async_count}/{TRAFFIC_GENERATION_COUNT} successful")
        print(f"\n  Check CloudWatch and DynamoDB metrics to observe the patterns.")


async def main(argv):
    num_requests = DEFAULT_REQUEST_COUNT

    try:
        opts, args = getopt.getopt(argv, "hsagn:", ["requests="])
    except getopt.GetoptError:
        print('Usage: python3 async_comparison.py [-s compare] [-a async-only] [-g generate traffic] [-n NUM]')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-n' or opt == '--requests':
            num_requests = int(arg)

    # Get endpoints
    print(f"\n  Module 9: Asynchronous Workflows")
    print(f"  Stack: {STACK_NAME}")
    print(f"  Retrieving endpoints...\n")

    urls = get_urls_from_cfn()
    print(f"  Orders endpoint:     {urls.get('OrdersEndpoint', 'NOT FOUND')}")
    print(f"  Processing endpoint: {urls.get('ProcessingEndpoint', 'NOT FOUND')}")

    if 'OrdersEndpoint' not in urls or 'ProcessingEndpoint' not in urls:
        print("\n  [ERROR] Required stack outputs not found.")
        print(f"  Ensure '{STACK_NAME}' is deployed and has OrdersEndpoint + ProcessingEndpoint outputs.")
        sys.exit(1)

    mode = 's'  # default to comparison
    for opt, arg in opts:
        if opt == '-h':
            print('Usage:')
            print('  python3 async_comparison.py -s        # Side-by-side comparison (default)')
            print('  python3 async_comparison.py -s -n 20  # 20 requests each')
            print('  python3 async_comparison.py -g        # Generate heavy traffic (1000 each)')
            sys.exit()
        elif opt == '-s':
            mode = 's'
        elif opt == '-a':
            mode = 'a'
        elif opt == '-g':
            mode = 'g'

    if mode == 's' or mode == 'a':
        await run_comparison(urls, num_requests)
    elif mode == 'g':
        await generate_traffic(urls)


if __name__ == "__main__":
    s = time.perf_counter()
    asyncio.run(main(sys.argv[1:]))
    elapsed = time.perf_counter() - s
    print(f"  Script executed in {elapsed:0.2f} seconds.\n")
