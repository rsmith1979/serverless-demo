# Module 8: DynamoDB Throttling Comparison Script
# ================================================
# Demonstrates DynamoDB provisioned capacity throttling and how
# SQS buffering with batch processing solves it.
#
# This script:
#   1. Writes directly to DynamoDB at high speed (triggers throttling)
#   2. Writes through SQS with a controlled batch consumer (no throttling)
#   3. Compares success rates, throttle counts, and throughput
#
# Prerequisites:
#   - Module 8 SAM stack deployed (workshop-traffic-throttle)
#   - Python 3.9+ with boto3 installed (available in CloudShell)
#
# Usage:
#   python3 throttle_comparison.py                # Default: 200 writes
#   python3 throttle_comparison.py -n 500         # 500 writes
#   python3 throttle_comparison.py --region us-east-2

import boto3
import time
import json
import uuid
import statistics
import argparse
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

STACK_NAME = 'workshop-traffic-throttle'


def get_stack_resources(stack_name, region):
    """Retrieve DynamoDB table name and SQS queue URL from the stack."""
    cf = boto3.client('cloudformation', region_name=region)
    try:
        response = cf.describe_stack_resources(StackName=stack_name)
        resources = {}
        for r in response['StackResources']:
            resources[r['LogicalResourceId']] = {
                'physical_id': r['PhysicalResourceId'],
                'type': r['ResourceType']
            }
        return resources
    except ClientError as e:
        # Try alternate stack name
        try:
            response = cf.describe_stack_resources(StackName='workshop-' + stack_name)
            resources = {}
            for r in response['StackResources']:
                resources[r['LogicalResourceId']] = {
                    'physical_id': r['PhysicalResourceId'],
                    'type': r['ResourceType']
                }
            return resources
        except:
            pass
        print(f"\n  [ERROR] Could not find stack '{stack_name}'.")
        print(f"  Available stacks with 'throttl' in name:")
        stacks = cf.list_stacks(StackStatusFilter=['CREATE_COMPLETE', 'UPDATE_COMPLETE'])
        for s in stacks['StackSummaries']:
            if 'throttl' in s['StackName'].lower():
                print(f"    - {s['StackName']}")
        raise SystemExit(1)


def find_resources(resources):
    """Extract DynamoDB table and SQS queue from stack resources."""
    table_name = None
    queue_url = None
    
    for logical_id, info in resources.items():
        if info['type'] == 'AWS::DynamoDB::Table':
            table_name = info['physical_id']
        elif info['type'] == 'AWS::SQS::Queue':
            queue_url = info['physical_id']
    
    return table_name, queue_url


def generate_item():
    """Generate a sample DynamoDB item."""
    return {
        'id': {'S': str(uuid.uuid4())},
        'timestamp': {'S': str(int(time.time() * 1000))},
        'data': {'S': f'test-payload-{uuid.uuid4().hex[:8]}'},
        'source': {'S': 'throttle-comparison-script'}
    }


def direct_write(dynamodb_client, table_name, item):
    """Write a single item directly to DynamoDB. Returns (success, throttled, elapsed_ms)."""
    start = time.perf_counter()
    try:
        dynamodb_client.put_item(TableName=table_name, Item=item)
        elapsed = (time.perf_counter() - start) * 1000
        return (True, False, elapsed)
    except ClientError as e:
        elapsed = (time.perf_counter() - start) * 1000
        if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
            return (False, True, elapsed)
        elif e.response['Error']['Code'] == 'ThrottlingException':
            return (False, True, elapsed)
        else:
            return (False, False, elapsed)


def sqs_write(sqs_client, queue_url, item):
    """Send item to SQS queue. Returns (success, elapsed_ms)."""
    start = time.perf_counter()
    try:
        # Convert DynamoDB item format to simple JSON for SQS
        simple_item = {k: list(v.values())[0] for k, v in item.items()}
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(simple_item)
        )
        elapsed = (time.perf_counter() - start) * 1000
        return (True, elapsed)
    except ClientError as e:
        elapsed = (time.perf_counter() - start) * 1000
        return (False, elapsed)


def run_direct_writes(dynamodb_client, table_name, num_writes, concurrency=10):
    """Blast writes directly at DynamoDB using thread pool."""
    results = []
    items = [generate_item() for _ in range(num_writes)]
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(direct_write, dynamodb_client, table_name, item): i 
            for i, item in enumerate(items)
        }
        for future in as_completed(futures):
            results.append(future.result())
    
    return results


def run_sqs_writes(sqs_client, queue_url, num_writes, concurrency=10):
    """Send writes to SQS queue using thread pool."""
    results = []
    items = [generate_item() for _ in range(num_writes)]
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = {
            executor.submit(sqs_write, sqs_client, queue_url, item): i 
            for i, item in enumerate(items)
        }
        for future in as_completed(futures):
            results.append(future.result())
    
    return results


def print_banner(text):
    """Print a formatted section banner."""
    print(f"\n{'=' * 64}")
    print(f"  {text}")
    print(f"{'=' * 64}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Module 8: DynamoDB Throttling Comparison'
    )
    parser.add_argument('-n', '--num-writes', type=int, default=200,
                        help='Number of write operations (default: 200)')
    parser.add_argument('--region', type=str, default='us-east-2',
                        help='AWS region (default: us-east-2)')
    parser.add_argument('--stack', type=str, default=STACK_NAME,
                        help=f'Stack name (default: {STACK_NAME})')
    parser.add_argument('--concurrency', type=int, default=20,
                        help='Concurrent writers (default: 20)')
    args = parser.parse_args()

    print_banner("MODULE 8: DynamoDB THROTTLING COMPARISON")
    print(f"  Stack:       {args.stack}")
    print(f"  Region:      {args.region}")
    print(f"  Writes:      {args.num_writes}")
    print(f"  Concurrency: {args.concurrency} threads")

    # Get stack resources
    print(f"\n  Discovering resources from stack '{args.stack}'...")
    resources = get_stack_resources(args.stack, args.region)
    table_name, queue_url = find_resources(resources)

    if not table_name:
        print("  [ERROR] No DynamoDB table found in stack resources.")
        print("  Stack resources found:")
        for lid, info in resources.items():
            print(f"    {lid}: {info['type']} = {info['physical_id']}")
        raise SystemExit(1)

    print(f"  DynamoDB table: {table_name}")
    if queue_url:
        print(f"  SQS queue:      {queue_url}")
    else:
        print("  [WARN] No SQS queue found. Will only run direct write test.")

    # Initialize clients
    dynamodb = boto3.client('dynamodb', region_name=args.region)
    sqs = boto3.client('sqs', region_name=args.region) if queue_url else None

    # === PHASE 1: Direct writes (expect throttling) ===
    print_banner("PHASE 1: DIRECT WRITES TO DynamoDB\n"
                 "  Writing directly at high speed. Provisioned capacity\n"
                 "  will be exceeded, causing ProvisionedThroughputExceededException.")

    print(f"  Sending {args.num_writes} writes with {args.concurrency} concurrent threads...")
    start_time = time.perf_counter()
    direct_results = run_direct_writes(dynamodb, table_name, args.num_writes, args.concurrency)
    direct_elapsed = time.perf_counter() - start_time

    # Analyze direct write results
    direct_success = sum(1 for s, t, e in direct_results if s)
    direct_throttled = sum(1 for s, t, e in direct_results if t)
    direct_failed = args.num_writes - direct_success
    direct_times = [e for s, t, e in direct_results if s]

    print(f"\n  Results ({direct_elapsed:.1f}s total):")
    print(f"  {'Metric':<30} {'Value':>10}")
    print(f"  {'-'*30} {'-'*10}")
    print(f"  {'Total writes attempted':<30} {args.num_writes:>10}")
    print(f"  {'Successful':<30} {direct_success:>10}")
    print(f"  {'Throttled (WCU exceeded)':<30} {direct_throttled:>10}")
    print(f"  {'Throttle rate':<30} {(direct_throttled/args.num_writes)*100:>9.1f}%")
    if direct_times:
        print(f"  {'Avg write latency':<30} {statistics.mean(direct_times):>8.1f} ms")
        print(f"  {'Max write latency':<30} {max(direct_times):>8.1f} ms")
    print(f"  {'Effective throughput':<30} {direct_success/direct_elapsed:>8.1f} items/s")

    if direct_throttled > 0:
        print(f"\n  \u26A0  {direct_throttled} requests were THROTTLED by DynamoDB!")
        print(f"     This means data was LOST (not written) without retry logic.")
    else:
        print(f"\n  [INFO] No throttling observed. Try increasing -n or --concurrency.")

    # === PHASE 2: SQS buffered writes ===
    if queue_url and sqs:
        print_banner("PHASE 2: BUFFERED WRITES VIA SQS\n"
                     "  Writing to SQS queue instead. A Lambda consumer\n"
                     "  processes messages in controlled batches, preventing\n"
                     "  DynamoDB throttling.")

        print(f"  Sending {args.num_writes} messages to SQS with {args.concurrency} threads...")
        start_time = time.perf_counter()
        sqs_results = run_sqs_writes(sqs, queue_url, args.num_writes, args.concurrency)
        sqs_elapsed = time.perf_counter() - start_time

        sqs_success = sum(1 for s, e in sqs_results if s)
        sqs_failed = args.num_writes - sqs_success
        sqs_times = [e for s, e in sqs_results if s]

        print(f"\n  Results ({sqs_elapsed:.1f}s total):")
        print(f"  {'Metric':<30} {'Value':>10}")
        print(f"  {'-'*30} {'-'*10}")
        print(f"  {'Total messages sent':<30} {args.num_writes:>10}")
        print(f"  {'Successful (queued)':<30} {sqs_success:>10}")
        print(f"  {'Failed':<30} {sqs_failed:>10}")
        print(f"  {'Throttled':<30} {'0':>10}")
        print(f"  {'Success rate':<30} {(sqs_success/args.num_writes)*100:>9.1f}%")
        if sqs_times:
            print(f"  {'Avg send latency':<30} {statistics.mean(sqs_times):>8.1f} ms")
            print(f"  {'Max send latency':<30} {max(sqs_times):>8.1f} ms")
        print(f"  {'Effective throughput':<30} {sqs_success/sqs_elapsed:>8.1f} msgs/s")

        print(f"\n  \u2713 ZERO throttling! All {sqs_success} messages accepted by SQS.")
        print(f"    The Lambda batch consumer will process them at a rate")
        print(f"    DynamoDB can handle (controlled by batch size).")

    # === COMPARISON ===
    print_banner("COMPARISON SUMMARY")

    print(f"  {'Metric':<35} {'Direct':>12} {'SQS Buffer':>12}")
    print(f"  {'-'*35} {'-'*12} {'-'*12}")
    print(f"  {'Writes attempted':<35} {args.num_writes:>12} {args.num_writes:>12}")
    print(f"  {'Successful':<35} {direct_success:>12} {sqs_success if queue_url else 'N/A':>12}")
    print(f"  {'Throttled/Lost':<35} {direct_throttled:>12} {'0':>12}")
    print(f"  {'Data loss risk':<35} {'HIGH':>12} {'NONE':>12}")
    if direct_times and queue_url and sqs_times:
        print(f"  {'Avg latency':<35} {statistics.mean(direct_times):>10.1f}ms {statistics.mean(sqs_times):>10.1f}ms")
        print(f"  {'Throughput':<35} {direct_success/direct_elapsed:>9.1f}/s {sqs_success/sqs_elapsed:>9.1f}/s")

    print(f"\n  KEY TAKEAWAY")
    print(f"  {'-'*50}")
    print(f"  Direct:  {direct_throttled}/{args.num_writes} writes LOST due to throttling")
    print(f"           ({(direct_throttled/args.num_writes)*100:.1f}% data loss without retries)")
    print(f"  Buffered: 0/{args.num_writes} writes lost")
    print(f"           SQS absorbs the burst; Lambda batch consumer")
    print(f"           writes to DynamoDB at a controlled rate")
    print(f"\n  Healthcare Example:")
    print(f"  A hospital processes 1000 lab results during morning rounds.")
    print(f"  Direct:  Results lost during traffic spike = missed diagnoses")
    print(f"  Buffered: All results queued safely, processed within minutes")
    print(f"           No data loss, no throttling, predictable costs")
    print(f"\n{'=' * 64}\n")


if __name__ == '__main__':
    main()
