import sqlite3
from flask import g, Flask, jsonify, render_template
from flask_cors import CORS
from functools import lru_cache
import ast

# DATABASE = '/home/teh_devs/bitcoinspy/tx.db'
DATABASE = 'tx.db'

TX_COLS = ['tx_val', 'n_inputs', 'n_outputs', 'block_height', 'is_coinbase']
INPUT_COLS = ['tx_val', 'prev_hash', 'prev_index']
OUTPUT_COLS = ['tx_val', 'value', 'address']
TX_MAP_COLS = ['val', 'hash']

@lru_cache(maxsize=512)
def get_address_status(address) -> str:
    # TODO
    import random
    i = random.randint(0, 3)
    if i == 0:
        return 'WHITE'
    if i == 3:
        return 'BLACK'
    return 'NEUTRAL'

@lru_cache(maxsize=256)
def tx_val_from_hash(tx_hash):
    cur = get_db().cursor()
    cur.execute(f"SELECT val FROM tx_map WHERE hash='{tx_hash}'")
    res = cur.fetchone()
    return res[0] if res else None

@lru_cache(maxsize=256)
def tx_hash_from_val(tx_val):
    cur = get_db().cursor()
    cur.execute(f"SELECT hash FROM tx_map WHERE val='{tx_val}'")
    res = cur.fetchone()
    return res[0] if res else None
    
@lru_cache(maxsize=256)
def output_from_tx_val(tx_val):
    cur = get_db().cursor()
    cur.execute(f"SELECT * FROM output WHERE tx_val='{tx_val}'")
    return cur.fetchall()

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

def get_address_from_output_obj(outp):
    addresses = ast.literal_eval(outp['address'])
    return addresses[0] if len(addresses) else ''

def get_tx_details(tx_hash):
    tx_val = tx_val_from_hash(tx_hash)
    if tx_val is None:
        return None
    cur = get_db().cursor()
    cur.execute(f"SELECT * FROM tx WHERE tx_val='{tx_val}'")
    res = cur.fetchone()
    if res is None:
        return None
    tx = {TX_COLS[i]: res[i] for i in range(len(TX_COLS))}
    n_inputs = tx['n_inputs']
    n_outputs = tx['n_outputs']
    is_coinbase = tx['is_coinbase']
    txn = {
        'block_height': tx['block_height'],
        'hash': tx_hash,
    }

    outputs = []
    index = -1
    cur.execute(f"SELECT tx_val, prev_index FROM input WHERE prev_hash='{tx_hash}'")
    prev_tx_vals = cur.fetchall()
    prev_tx_vals = {x[1]: x[0] for x in prev_tx_vals}
    for outp in output_from_tx_val(tx_val):
        index += 1
        outp = {OUTPUT_COLS[i]: outp[i] for i in range(len(OUTPUT_COLS))}
        amount = outp['value']
        if amount == 0:
            # Often occurs when type is 'OP_RETURN'
            continue
        prev_tx_val = prev_tx_vals.get(index)
        outputs.append({
            'receiver_address': get_address_from_output_obj(outp),
            'amount': amount,
            'next_tx_hash': tx_hash_from_val(prev_tx_val) if prev_tx_val is not None else '',
        })
    if index + 1 != n_outputs:
        print(f"Mismatch in the number of outputs for tx: {tx_hash} {tx_val} {index+1} != {n_outputs}")
    output_sum = sum(x['amount'] for x in outputs)
    txn['outputs'] = outputs

    inputs = []
    if is_coinbase:
        inputs = [{
            'prev_tx_hash': '',
            'sender_address': 'COINBASE',
            'amount': output_sum,
        }]
    else:
        for inp in cur.execute(f"SELECT * FROM input WHERE tx_val='{tx_val}'"):
            inp = {INPUT_COLS[i]: inp[i] for i in range(len(INPUT_COLS))}
            prev_hash = inp['prev_hash']
            prev_tx_val = tx_val_from_hash(prev_hash)
            prev_index = inp['prev_index']
            res = output_from_tx_val(prev_tx_val)
            if prev_index < len(res):
                o = res[prev_index]
                o = {OUTPUT_COLS[i]: o[i] for i in range(len(OUTPUT_COLS))}
                inputs.append({
                    'prev_tx_hash': prev_hash,
                    'sender_address': get_address_from_output_obj(o),
                    'amount': o['value'],
                })
            else:
                # Incomplete blockchain data
                pass
    input_sum = sum(x['amount'] for x in inputs)
    if len(inputs) != n_inputs:
        print(f"Mismatch in the number of inputs for tx: {tx_hash} {tx_val} {len(inputs)} != {n_inputs}")
    txn['inputs'] = inputs
    txn['fees'] = 0 if is_coinbase else input_sum - output_sum
    return txn

def get_coinbase_txn_hash(block_height):
    cur = get_db().cursor()
    cur.execute(f"SELECT * FROM tx WHERE is_coinbase=1 AND block_height={block_height};")
    res = cur.fetchone()
    return None if res is None else res[0]

def get_tx_input_txns(txn):
    inp_hashes = {x['prev_tx_hash'] for x in txn['inputs']}
    txns = []
    for inp in txn['inputs']:
        if inp['prev_tx_hash'] != '':
            i = get_tx_details(inp['prev_tx_hash'])
            assert i['hash'] in inp_hashes
            txns.append(i)
    assert len(txns) <= len(inp_hashes)
    return txns

def get_tx_output_txns(txn):
    outp_hashes = {x['next_tx_hash'] for x in txn['outputs']}
    txns = []
    for outp in txn['outputs']:
        if outp['next_tx_hash'] != '':
            o = get_tx_details(outp['next_tx_hash'])
            assert o['hash'] in outp_hashes
            txns.append(o)
    assert len(txns) <= len(outp_hashes)
    return txns

def get_tx_graph_array(txn, output_levels=2):
    txns = [txn]
    input_txns = get_tx_input_txns(txn)
    txns.extend(input_txns)
    output_txns = get_tx_output_txns(txn)
    txns.extend(output_txns)
    for _ in range(1, output_levels):
        next_level = []
        for output_txn in output_txns:
            outputs = get_tx_output_txns(output_txn)
            next_level.extend(outputs)
        txns.extend(next_level)
        output_txns = next_level
    return txns

def get_blacklist_whitelist_from_tx_array(tx_array):
    blacklisted_addresses = []
    whitelisted_addresses = []
    addresses = set()
    for tx in tx_array:
        for inp in tx['inputs']:
            addresses.add(inp['sender_address'])
        for outp in tx['outputs']:
            addresses.add(outp['receiver_address'])
    for address in addresses:
        if address == "COINBASE":
            continue
        status = get_address_status(address)
        if status == 'WHITE':
            whitelisted_addresses.append(address)
        elif status == 'BLACK':
            blacklisted_addresses.append(address)
    return blacklisted_addresses, whitelisted_addresses

def data_from_tx_array(tx_array):
    blacklist, whitelist = get_blacklist_whitelist_from_tx_array(tx_array)
    return {
        'txns': tx_array,
        'blacklist': blacklist,
        'whitelist': whitelist,
    }

app = Flask(__name__)
CORS(app)

@app.route('/')
def main_view():
    return render_template('index.html')

@app.route('/tx_data/<hash>')
def tx_data_route(hash):
    return jsonify(get_tx_details(hash))

@app.route('/tx/<hash>')
def tx_graph_route(hash):
    # return {
    #     "blacklist":["o"*34],
    #     "whitelist":["white"],
    #     "txns":[
    #         {
    #             "block_height":1,"fees":0,"hash":"M",
    #             "inputs":[
    #                 {"amount":12,"prev_tx_hash":"I1","sender_address":"a"*34},
    #                 {"amount":18,"prev_tx_hash":"I2","sender_address":"o"*34},
    #             ],"outputs":[
    #                 {"amount":11,"next_tx_hash":"","receiver_address":"e"*34}, # utxo
    #                 {"amount":10,"next_tx_hash":"O2","receiver_address":"g"*34},
    #                 {"amount":9,"next_tx_hash":"","receiver_address":"i"*34}, # utxo
    #             ]
    #         },
    #         {
    #             "block_height":1,"fees":0,"hash":"I2",
    #             "inputs":[
    #                 {"amount":7,"prev_tx_hash":"","sender_address":"COINBASE"},
    #                 {"amount":19,"prev_tx_hash":"I2I2","sender_address":"m"*34},
    #             ],"outputs":[
    #                 {"amount":18,"next_tx_hash":"M","receiver_address":"o"*34},
    #                 {"amount":8,"next_tx_hash":"","receiver_address":"u"*34}, # utxo
    #             ]
    #         },
    #         {
    #             "block_height":1,"fees":0,"hash":"O2",
    #             "inputs":[
    #                 {"amount":10,"prev_tx_hash":"M","sender_address":"g"*34},
    #             ],"outputs":[
    #                 {"amount":4,"next_tx_hash":"O2O1","receiver_address":"q"*34},
    #                 {"amount":6,"next_tx_hash":"O2O2","receiver_address":"s"*34},
    #             ]
    #         },
    #         {
    #             "block_height":1,"fees":0,"hash":"I2I2",
    #             "inputs":[
    #                 {"amount":20,"prev_tx_hash":"I2I2I1","sender_address":"y"*34},
    #             ],"outputs":[
    #                 {"amount":19,"next_tx_hash":"I2","receiver_address":"m"*34},
    #                 {"amount":1,"next_tx_hash":"","receiver_address":"y"*34}, # utxo
    #             ]
    #         },
    #         {
    #             "block_height":1,"fees":0,"hash":"O2O1",
    #             "inputs":[
    #                 {"amount":4,"prev_tx_hash":"O2","sender_address":"q"*34},
    #             ],"outputs":[
    #                 {"amount":4,"next_tx_hash":"O2O1O1","receiver_address":"w"*34},
    #             ]
    #         },
    #         {
    #             "block_height":1,"fees":0,"hash":"I2I2I1",
    #             "inputs":[
    #                 {"amount":20,"prev_tx_hash":"I2I2I1I1","sender_address":"x"*34},
    #             ],"outputs":[
    #                 {"amount":20,"next_tx_hash":"I2I2","receiver_address":"y"*34},
    #             ]
    #         },
    #     ],
    # }
    txn = get_tx_details(hash)
    tx_graph_array = get_tx_graph_array(txn)
    return jsonify(data_from_tx_array(tx_graph_array))

@app.route('/block_coinbase/<block_height>')
def block_coinbase_data_route(block_height):
    coinbase_txn_hash = get_coinbase_txn_hash(block_height)
    if coinbase_txn_hash is None:
        return jsonify(None)
    coinbase_txn = get_tx_details(coinbase_txn_hash)
    tx_graph_array = get_tx_graph_array(coinbase_txn)
    return jsonify(data_from_tx_array(tx_graph_array))

@app.route('/dbstatus')
def database_status():
    try:
        conn = get_db()
        with conn:
            maxb = conn.execute("SELECT MAX(block_height) from tx;").fetchone()[0]
            minb = conn.execute("SELECT MIN(block_height) from tx;").fetchone()[0]
        return f'Current minimum block height in db -> <a href="https://blockchair.com/bitcoin/block/{minb}">{minb}</a>' + f'<br/>Current maximum block height in db -> <a href="https://blockchair.com/bitcoin/block/{maxb}">{maxb}</a>'
    except sqlite3.OperationalError:
        return "DB is locked, currently being updated, please try again after some time"


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()
