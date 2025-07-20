import json
import boto3
import csv
import io
import os
from datetime import datetime
from typing import List, Dict, Any
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    """
    Universal DynamoDB Backend for Wedding Guest Tables - WITH DEBUG LOGGING
    """
    # DEBUG: print incoming event
    print("=== RAW EVENT DEBUG ===")
    print(f"Event type: {type(event)}")
    print(f"Event content: {json.dumps(event, indent=2, default=str)}")
    print("=====================")
    
    # Resolve action and table_name
    action = event.get('action', 'get_first')
    table_name = event.get('table_name')
    
    dynamodb = boto3.client('dynamodb')
    s3       = boto3.client('s3')
    
    try:
        # Handle API Gateway payloads
        if 'body' in event:
            print("=== API GATEWAY EVENT DETECTED ===")
            raw = event['body']
            print(f"Raw body: {raw}")
            if raw:
                body = json.loads(raw)
                print(f"Parsed body: {json.dumps(body, indent=2)}")
                action     = body.get('action', action)
                table_name = body.get('table_name', table_name)
                event.update(body)
            else:
                print("Empty body received")
        
        # Fallback to env var if needed
        if not table_name:
            table_name = os.environ.get('DYNAMODB_TABLE_NAME')
            if not table_name:
                raise ValueError("table_name is required (payload or DYNAMODB_TABLE_NAME)")
        
        print(f"Final action: {action}, table: {table_name}")
        
        # Dispatch to handler
        if action == 'get_first':
            return handle_get_first(dynamodb, table_name)
        elif action == 'get_all':
            return handle_get_all(dynamodb, table_name)
        elif action == 'add_guest':
            return handle_add_guest(dynamodb, table_name, event)
        elif action == 'update_guest':
            return handle_update_guest(dynamodb, table_name, event)
        elif action == 'delete_guest':
            return handle_delete_guest(dynamodb, table_name, event)
        elif action == 'export_csv':
            return handle_export_csv(dynamodb, s3, table_name, event)
        elif action == 'count':
            return handle_count(dynamodb, table_name)
        elif action == 'clear_table':
            return handle_clear_table(dynamodb, table_name)
        else:
            raise ValueError(f"Unknown action: {action}")
            
    except Exception as e:
        error_msg = f"Error processing {action}: {str(e)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
                'Access-Control-Allow-Headers': '*'
            },
            'body': json.dumps({
                'error': str(e),
                'action': action,
                'table': table_name,
                'debug_event': event
            })
        }


def handle_get_first(dynamodb, table_name: str):
    """Get and echo the first record from the table."""
    response = dynamodb.scan(TableName=table_name, Limit=1)
    items = response.get('Items', [])
    if not items:
        msg = f"Table '{table_name}' is empty"
        print(msg)
        return _make_response(200, {'message': msg, 'record': None})
    
    record = convert_dynamodb_item(items[0])
    print(f"=== FIRST RECORD ===\n{record}\n==================")
    return _make_response(200, {
        'message': 'First record retrieved successfully',
        'table': table_name,
        'record': record
    })


def handle_get_all(dynamodb, table_name: str):
    """Get all records from the table."""
    items = scan_all_items(dynamodb, table_name)
    records = [convert_dynamodb_item(item) for item in items]
    print(f"Retrieved {len(records)} records from {table_name}")
    return _make_response(200, {
        'message': f'Retrieved {len(records)} records',
        'table': table_name,
        'count': len(records),
        'records': records
    })


def handle_add_guest(dynamodb, table_name: str, event: dict):
    """
    Add a new guest to the table, capturing both the guest's name and 
    the number of accompanying guests for accurate headcount tracking.
    """
    guest = event.get('guest', {})
    name  = guest.get('name')
    count = int(guest.get('guests', 0))              # default to 0 if unset

    # Validate mandatory field
    if not name:
        raise ValueError("Guest name is required")

    # Prevent duplicate entries by name + guest count
    resp = dynamodb.scan(
        TableName=table_name,
        FilterExpression='guest_name = :name AND guests = :guests',
        ExpressionAttributeValues={
            ':name':   {'S': name},
            ':guests': {'N': str(count)}
        }
    )
    if resp.get('Items'):
        raise ValueError(f"Guest '{name}' with {count} guests already exists")

    # Use name as primary key (since DynamoDB table uses name as hash key)
    key = name

    # Build item with the new 'guests' numeric attribute
    item = {
        'name':            {'S': key},
        'guest_name':      {'S': name},
        'guests':          {'N': str(count)},
        'attendingstatus': {'S': guest.get('attendingstatus', 'no')},
        'comments':        {'S': guest.get('comments', '')}
    }

    # Persist to DynamoDB
    dynamodb.put_item(
        TableName=table_name,
        Item=item,
        ConditionExpression='attribute_not_exists(#n)',
        ExpressionAttributeNames={'#n': 'name'}
    )

    # Return a consistent, JSON‑serializable response
    return _make_response(200, {
        'message':    f"Guest '{name}' added successfully",
        'table':      table_name,
        'guest': {
            'name':             name,
            'guests':           count,
            'attendingstatus':  guest.get('attendingstatus', 'no'),
            'comments':         guest.get('comments', '')
        },
        'unique_id':  key
    })


def handle_update_guest(dynamodb, table_name: str, event: dict):
    """Update an existing guest."""
    name    = event.get('name')
    updates = event.get('updates', {})
    if not name:
        raise ValueError("Guest name is required for updates")
    if not updates:
        raise ValueError("No updates provided")
    
    # Use name as the key since that's what the DynamoDB table uses
    key = name
    
    # Build expressions
    exprs, names, values = [], {'#n': 'name'}, {}
    for k, v in updates.items():
        if k in ['name', 'guest_name']:
            continue
        ph, pv = f"#{k}", f":{k}"
        exprs.append(f"{ph} = {pv}")
        names[ph] = k
        # If updating 'guests', store as Number
        if k == 'guests':
            values[pv] = {'N': str(v)}
        else:
            values[pv] = {'S': str(v)}
    update_expr = "SET " + ", ".join(exprs)
    
    dynamodb.update_item(
        TableName=table_name,
        Key={'name': {'S': key}},
        UpdateExpression=update_expr,
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
        ConditionExpression='attribute_exists(#n)'
    )
    print(f"Updated guest: {name} (key: {key})")
    return _make_response(200, {
        'message':       f"Guest '{name}' updated successfully",
        'table':         table_name,
        'updates':       updates,
        'unique_key':    key
    })


def handle_delete_guest(dynamodb, table_name: str, event: dict):
    """Delete a guest from the table."""
    name = event.get('name')
    if not name:
        raise ValueError("Guest name is required for deletion")
    
    # Use name as the key since that's what the DynamoDB table uses
    key = name
    dynamodb.delete_item(
        TableName=table_name,
        Key={'name': {'S': key}},
        ConditionExpression='attribute_exists(#n)',
        ExpressionAttributeNames={'#n': 'name'}
    )
    print(f"Deleted guest: {name} (key: {key})")
    return _make_response(200, {
        'message':    f"Guest '{name}' deleted successfully",
        'table':      table_name,
        'unique_key': key
    })


def handle_export_csv(dynamodb, s3, table_name: str, event: dict):
    """Export table to CSV in S3."""
    bucket = os.environ.get('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME env var is required")
    
    items = scan_all_items(dynamodb, table_name)
    if not items:
        return _make_response(200, {
            'message': f"Table '{table_name}' is empty – no CSV created",
            'table': table_name
        })
    
    csv_str = convert_to_csv(items)
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    key     = event.get('s3_key', f"wedding-exports/{table_name}_export_{ts}.csv")
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_str,
        ContentType='text/csv',
        Metadata={
            'source_table':    table_name,
            'export_timestamp': ts,
            'items_count':      str(len(items))
        }
    )
    url = f"s3://{bucket}/{key}"
    print(f"Exported {len(items)} records to {url}")
    return _make_response(200, {
        'message':         'CSV export completed successfully',
        'table':           table_name,
        's3_location':     url,
        'items_exported':  len(items),
        'timestamp':       ts
    })


def handle_count(dynamodb, table_name: str):
    """Get total count of guests in the table."""
    resp = dynamodb.scan(TableName=table_name, Select='COUNT')
    cnt  = resp.get('Count', 0)
    print(f"Table '{table_name}' count: {cnt}")
    return _make_response(200, {
        'message': f"Table contains {cnt} guests",
        'table':   table_name,
        'count':   cnt
    })


def handle_clear_table(dynamodb, table_name: str):
    """
    Clear all records from the specified DynamoDB table.
    WARNING: irreversible bulk delete.
    """
    items = scan_all_items(dynamodb, table_name)
    total = len(items)
    if total == 0:
        msg = f"Table '{table_name}' is already empty."
        print(msg)
        return _make_response(200, {'message': msg, 'deleted_count': 0})
    
    # Batch‑delete in 25‑item chunks
    for i in range(0, total, 25):
        batch = items[i:i+25]
        requests = [
            {'DeleteRequest': {'Key': {'name': itm['name']}}}
            for itm in batch
        ]
        dynamodb.batch_write_item(RequestItems={table_name: requests})
        print(f"Deleted batch {i//25 + 1}: {len(batch)} items")
    
    msg = f"Cleared table '{table_name}' – deleted {total} items."
    print(msg)
    return _make_response(200, {'message': msg, 'deleted_count': total})


# ──────── Utility functions ───────────────────────────────────────────────────

def scan_all_items(dynamodb, table_name: str) -> List[Dict[str, Any]]:
    """Scan entire table with pagination."""
    items, kwargs = [], {'TableName': table_name}
    while True:
        resp = dynamodb.scan(**kwargs)
        items.extend(resp.get('Items', []))
        if 'LastEvaluatedKey' not in resp:
            break
        kwargs['ExclusiveStartKey'] = resp['LastEvaluatedKey']
    return items


def convert_dynamodb_item(item: Dict[str, Any]) -> Dict[str, str]:
    """Convert DynamoDB item format to simple key-value pairs."""
    out = {}
    for k, v in item.items():
        if 'S' in v:     out[k] = v['S']
        elif 'N' in v:   out[k] = v['N']
        elif 'BOOL' in v: out[k] = str(v['BOOL']).lower()
        elif 'NULL' in v: out[k] = ''
        else:            out[k] = str(v)
    out['display_name'] = out.get('guest_name', out.get('name', ''))
    return out


def convert_to_csv(items: List[Dict[str, Any]]) -> str:
    """Convert DynamoDB items to CSV string."""
    rows = [convert_dynamodb_item(it) for it in items]
    all_fields = set().union(*rows)
    
    # Preferred order
    pref = ['display_name', 'attendingstatus', 'comments', 'guests']
    fields = [f for f in pref if f in all_fields]
    all_fields -= set(fields)
    fields += sorted(all_fields - {'name', 'guest_name'})
    csv_fields = ['name' if f=='display_name' else f for f in fields]
    
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=csv_fields)
    writer.writeheader()
    for r in rows:
        writer.writerow({csv_fields[i]: r.get(fields[i], '') for i in range(len(fields))})
    return buf.getvalue()


def _resolve_unique_key(dynamodb, table_name: str, name: str, phone: str) -> str:
    """Determine the item's primary key for update/delete."""
    # Since we're now using just name as the key, return the name
    return name


def _make_response(status: int, body: Dict[str, Any]):
    """Standardize HTTP JSON response."""
    return {
        'statusCode': status,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body)
    }