from os import environ
import asyncio
import aiohttp
from pandas import DataFrame, to_datetime
import numpy

API_KEY = environ['COINGLASS_API_KEY']


symbols = [   # https://fapi.coinglass.com/api/support/symbol
    '1000BONK', '1000LUNC', '1000SHIB', '1000XEC', '1INCH',
    'AAVE', 'ACH', 'ADA', 'AGIX', 'AGLD', 'ALGO', 'ALICE', 'ALPHA',
    'AMB', 'ANKR', 'ANT', 'APE', 'API3', 'APT', 'AR', 'ARPA', 'ASTR',
    'ATA', 'ATOM', 'AUDIO', 'AVAX', 'AXS',
    'BAKE', 'BAL', 'BAND', 'BAT', 'BCH', 'BEL', 'BICO', 'BIT', 'BLUEBIRD',
    'BLZ', 'BMEX', 'BNB', 'BNT', 'BNX', 'BSV', 'BSW', 'BTC', 'BTCDOM',
    'C98', 'CEL', 'CELO', 'CELR', 'CFX', 'CHR', 'CHZ', 'COMP', 'COTI',
    'CREAM', 'CRO', 'CRV', 'CSPR', 'CTK', 'CTSI', 'CVC', 'CVX',
    'DAR', 'DASH', 'DEFI', 'DENT', 'DGB', 'DODO', 'DOGE', 'DOME',
    'DORA', 'DOT', 'DUSK', 'DYDX',
    'EGLD', 'ENJ', 'ENS', 'EOS', 'ETC', 'ETH', 'ETHW',
    'FET', 'FIL', 'FITFI', 'FLM', 'FLOW', 'FOOTBALL', 'FTM', 'FXS',
    'GAL', 'GALA', 'GLMR', 'GMT', 'GMX', 'GODS', 'GRT', 'GTC',
    'HBAR', 'HNT', 'HOOK', 'HOT', 'HT',
    'ICP', 'ICX', 'ILV', 'IMX', 'INJ', 'IOST', 'IOTA', 'IOTX',
    'JASMY', 'JST',
    'KAVA', 'KDA', 'KISHU', 'KLAY', 'KNC', 'KSM',
    'LDO', 'LEVER', 'LINA', 'LINK', 'LIT', 'LOOKS', 'LPT', 'LRC',
    'LTC', 'LUNA', 'LUNA2', 'LUNC',
    'MAGIC', 'MANA', 'MASK', 'MATIC', 'MINA', 'MKR', 'MTL',
    'NEAR', 'NEO', 'NFT', 'NKN',
    'OCEAN', 'OGN', 'OMG', 'ONE', 'ONT', 'OP',
    'PAXG', 'PEOPLE', 'PERP', 'PHB',
    'QNT', 'QTUM',
    'REEF', 'REN', 'RLC', 'RNDR', 'ROSE', 'RSR', 'RSS3', 'RUNE', 'RVN',
    'SAND', 'SFP', 'SHIB', 'SHIB1000', 'SKL', 'SLP', 'SNX', 'SOL', 'SPELL',
    'STARL', 'STG', 'STMX', 'STORJ', 'STX', 'SUSHI', 'SWEAT', 'SXP',
    'T', 'THETA', 'TLM', 'TOMO', 'TON', 'TRB', 'TRX', 'TWT',
    'UMA', 'UNFI', 'UNI', 'USDC', 'USTC',
'VET',
    'WAVES', 'WOO',
'XCH', 'XCN', 'XEM', 'XLM', 'XMR', 'XNO', 'XRP', 'XTZ',
    'YFI', 'YFII', 'YGG',
    'ZEC', 'ZEN', 'ZIL', 'ZRX',

    'ALL'
]

exchanges = {
    'Binance',
    'Bitmex',
    'Bybit',
    'Bitget',
    'Bitfinex',
    'Coinbase Pro',
    'CoinEx',
    'CME',
    'Deribit',
    'dYdX',
    'Huobi',
    'Gate',
    'Gemini',
    'Kraken',
    'LedgerX',
    'OKX',
    'CME',
    'Bit.com',
}

def ex(exchange):
    assert exchange in exchanges
    return exchange

def symbol(s):
    assert s in symbols, s
    return s

intervals = {*('m1 m5 m15 m30 h1 h4 h8 h12 h24'.split())}
def interval(t):
    assert t in intervals, t
    return t

def time_type(t):
    assert t in intervals, t
    return t

def time_checker(name):
    def check_time(t):
        if isinstance(t, (int, float, numpy.int64)):
            return int(t)
        if isinstance(t, datetime):
            return int(t.timestamp() * 1000)
    check_time.__name__ = name
    return check_time
start_time = time_checker('start_time')
end_time = time_checker('end_time')

currencies = {'USD', 'BTC'}
def currency(c):
    assert c in currencies, c
    return c

def pair(p):
    return p

def limit(l):
    assert (l := int(l)) < 500, l
    return l

class CoinglassException(Exception): pass

def endpoints(
    base_URL,
    **named_endpoints
):
    def make_endpoint_functions(
        name,
        parameters,
        order,
        convert=False,
        rename={},
        remove=False
    ):
        if remove:
            nl = '\n'
            g = {'remove': remove}
            exec(
                f'def remove_data(data):{nl}'
                f'    {f"{nl}    ".join([f"try:{nl}        del data[{repr(k)}]{nl}    except: pass" for k in remove])}{nl}'
                f'    return data{nl}',
                g
            )
            remove_data = g['remove_data']
        if rename:
            q = "'"
            def rename_data(data):
                return {
                    rename.get(old, old): value for old, value in data.items()
                }
        async def endpoint_function(
            session,
            rename_fields=True,
            remove_unused=True,
            timeout=30,
            **kwargs
        ):
            try:
                arguments = {
                    parameter: parameters[parameter](value)
                    for parameter, value in kwargs.items()
                }
            except KeyError as key_error:
                raise TypeError(
                    f"{key_error.args[0]} is invalid for this endpoint."
                    f" Valid arguments are {', '.join(parameters.keys())}."
                    " also keywords remove_unused and rename_fields."
                )
            url = base_URL + name
            response = await session.get(
                url,
                params=arguments,
                timeout=30
            )
            response_json = await response.json()
            try:
                success = response_json['success']
            except:
                raise CoinglassException(response)
            else:
                if not response_json["success"]:
                    raise CoinglassException(
                        f"{url}: Code {response_json['code']}: {response_json['msg']}"
                    )
            data = response_json["data"]
            if convert:
                data = convert(data, **kwargs)
            if remove_unused and remove:
                data = map(remove_data, data)
            if rename_fields and rename:
                data = map(rename_data, data)
            return data

        async def endpoint_dataframe(session, order_field=None, rename_fields=True, **kwargs):
            order_field = order_field or order
            if rename_fields:
                order_field = rename.get(order_field, order_field)
            return as_dataframe(
                endpoint_function(session, rename_fields=rename_fields, **kwargs),
                order_field
            )

        endpoint_function.__qualname__ = endpoint_function.__name__ = name
        name_dataframe = name+'_dataframe'
        endpoint_dataframe.__qualname__ = name_dataframe
        setattr(CoinglassSession, name, endpoint_function)
        setattr(CoinglassSession, name_dataframe, endpoint_dataframe)

    for name, (
        parameters,
        order,
        *specs
    ) in named_endpoints.items():
        specs, = specs or ({},)
        make_endpoint_functions(
            name,
            {p.__name__:p for p in parameters},
            order,
            **specs,
        )


def as_dataframe(sequence_of_dicts, index_field):
    data_frame = DataFrame(sequence_of_dicts)
    if index_field:
        data_frame.set_index(index_field, inplace=True, drop=True)
    return data_frame


class CoinglassSession(aiohttp.ClientSession):
    def __init__(session, **kwargs):
        headers={
            "accept": "application/json",
            "coinglassSecret": API_KEY,
        }
        headers.update(kwargs.pop(headers, {}))
        super().__init__(headers=headers, **kwargs)


filter_by_market = {ex, pair, interval, limit, start_time, end_time}
filter_by_symbol = {symbol, interval, limit, start_time, end_time}

def min_field_count(n):
    def filter_by_field_count(data, **_):
        for d in data:
            if len(d) >= n:
                yield d
    return filter_by_field_count

convert = 'convert'
remove = 'remove'
rename = 'rename'
all_fields = {
    's':'symbol',
    'o': 'open',
    'c': 'close',
    'h': 'high',
    'l': 'low',
    't':'time',
    'sellQty': 'sell_quantity',
    'buyQty': 'buy_quantity',
    'buyVolUsd': 'buy_volume_USD',
    'sellVolUsd': 'sell_volume_USD',
    'turnoverNumber': 'turnover_number',
    'buyTurnoverNumber': 'buy_turnover_number',
    'sellTurnoverNumber': 'sell_turnover_number',
    'volUsd': 'volume_USD',
    'createTime': 'time',
    'updateTime': 'time',
    'fundingRate': 'funding_rate',
    'createTime':'time',
    'longRatio':'long_ratio',
    'shortRatio':'short_ratio',
    'longShortRatio':'long_short_ratio',
    'exchangeName': 'exchange',
    'originalSymbol': 'original_symbol',
    'quoteCurrency': 'quote_currency',
    'turnoverNumber': 'turnover_number',
    'longRate': 'long_rate',
    'longVolUsd': 'long_volume_USD',
    'shortVolUsd': 'short_volume_USD',
    'shortRate': 'short_rate',
    'openPrice': 'open_price',
    'priceChange': 'price_change',
    'priceChangePercent': 'price_change_percent',
    'indexPrice': 'index_price',
    'nextFundingTime': 'next_funding_time',
    'predictedRate': 'predicted_rate',
    'expiryDate': 'expiry_date',
    'totalVolUsd': 'total_volume_USD',
    'highPrice': 'high_price',
    'lowPrice': 'low_price',
    'openInterestAmount': 'open_interest_amount',
    'openInterest': 'open_interest',
    'h1OIChangePercent': 'oi_change_percent_10_hr',
    'h4OIChangePercent': 'oi_change_percent_4_hr',
    'h24Change': 'change_24_hr',
    'volChangePercent': 'volume_change_percent',
    'avgFundingRate': 'average_funding_rate',
    'oIChangePercent': 'oi_change_percent',
    'averagePrice': 'average_price',
}

unused_long_short = {
    'exchangeName', 'originalSymbol', 'symbol', 'quoteCurrency', 'type'
}


endpoints(
    'https://open-api.coinglass.com/public/v2/indicator/',
    funding=(filter_by_market, 'createTime', {
        convert:min_field_count(4),
    }),
    funding_ohlc=(filter_by_market, 't', {
        rename:all_fields,
        remove:{'s'}
    }),
    funding_avg=(filter_by_symbol, 'createTime', {
        rename: all_fields,
        remove: {'symbol', 'quoteCurrency', 'exchangeName', 't'},
    }),
    open_interest_ohlc=(filter_by_market, 't', {rename:all_fields}),
    open_interest_aggregated_ohlc=(filter_by_symbol, 't'),
    liquidation_symbol=(filter_by_symbol, 'createTime', {
        rename: all_fields
    }),
    liquidation_pair=(filter_by_market, 't', {
        rename: all_fields
    }),
    long_short_accounts=(filter_by_market, 'createTime', {
        rename: all_fields,
        remove: unused_long_short,
    }),
    long_short_symbol=(filter_by_symbol, 't',{
        rename: {'t': 'time', 'v': 'long_short_ratio'}
    }),
)

def access_by_symbol(data, symbol, **kwargs):
    for d in data[symbol]:
        d.pop('exchangeLogo')
        d.pop('symbolLogo')
        yield d

def history_converter(list_names):
    listed_data_field_names = [*list_names]
    def convert_history(data, **_):
        if isinstance(data, list):
            data, = data
        dates = data['dateList']
        if not dates:
            return
        data_lists_by_exchange = [
            data[list_name]
            for list_name in list_names.values()
        ]
        prices = data['priceList']
        exchanges = set(data['dataMap'])
        for exchange in exchanges:
            field_datas = [
                data_list[exchange]
                for data_list in data_lists_by_exchange
            ]
            for date,  price,  *field_data in zip(
                dates, prices, *field_datas
            ):
                yield {
                    'exchange': exchange,
                    'time': date,
                    'price': price,
                    **dict(zip(listed_data_field_names, field_data))
                }
    return convert_history


convert_funding_rate_history = history_converter({
    'data':'dataMap',
    'funding_rate':'frDataMap',
})

convert_open_interest_history = history_converter({
    'open_interest': 'dataMap'
})

convert_option_history = history_converter({
    'data': 'dataMap'
})


def convert_liquidations_history(data, **_):
    for summary in data:
        for exchange_data in summary['list']:
            exchange_data.update(
                time=summary['createTime'],
                price=summary['price'],
            )
            yield exchange_data

symbol_only = {symbol}
symbol_and_interval = {symbol, time_type}

endpoints(
    'https://open-api.coinglass.com/public/v2/',
    perpetual_market=(symbol_only, 'updateTime', {
        convert: access_by_symbol,
        rename: all_fields,
        remove: ('symbol')
    }),
    futures_market=(symbol_only, 'updateTime', {
        rename: all_fields,
        convert: access_by_symbol,
        remove: {'symbol'}
    }),
    #funding=({}, ''),
    funding_usd_history=(symbol_and_interval, 'time', {
        convert: convert_funding_rate_history,
        remove: {'data'},
    }),
    funding_coin_history=(symbol_and_interval, 'time', {
        convert: convert_funding_rate_history,
        remove: {'data'}
    }),
    open_interest=(symbol_only, 'exchangeName', {
        rename: all_fields,
        remove: {'exchangeLogo'},
    }),
    open_interest_history=({symbol, time_type, currency}, 'time', {
        convert: convert_open_interest_history
    }),
    option=(symbol_only, 'exchangeName', {
        rename: all_fields,
        remove: {'symbol', 'exchangeLogo'},
    }),
    option_history=({symbol, currency}, 'time', {
        convert: convert_option_history
    }),
    liquidation_top=({time_type}, 'symbol', {
        remove: {'symbolLogo'},
        rename: all_fields,
    }),
    liquidation_info=(symbol_and_interval, 'h1Amount', {
        remove: 'maxLiquidationOrder'
    }),
    liquidation_ex=(symbol_and_interval, 'exchangeName', {
        remove: {'exchangeLogo'},
        rename: all_fields
    }),
    liquidation_history=(symbol_and_interval, 'createTime', {
        convert: convert_liquidations_history,
        rename: all_fields,
    }),
    long_short=(symbol_and_interval, 'createTime', {
        remove: {'symbol', 'exchangeLogo', 'symbolLogo', },
        convert: (lambda data, **_: data[0]['list']),
        rename:all_fields
    }),
    long_short_history=(symbol_and_interval, 'time', {
        convert: lambda data, **_: ({
            'long_rate': long_rate,
            'short_rate': short_rate,
            'long_short_rate': long_short_rate,
            'time': date,
#            'price': price  # seems to be broken
        } for long_rate, short_rate, long_short_rate, date in zip(
            data['longRateList'],
            data['shortsRateList'],
            data['longShortRateList'],
            data['dateList'],
 #           data['priceList'],
        ))
    })
)
del CoinglassSession.liquidation_info_dataframe

