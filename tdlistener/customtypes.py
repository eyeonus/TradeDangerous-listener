# Copyright (C) Oliver 'kfsone' Smith <oliver@kfs.org> 2015
#
# Conditional permission to copy, modify, refactor or use this
# code is granted so long as attribution to the original author
# is included.
from collections import namedtuple

class MarketPrice(namedtuple('MarketPrice', [
        'system',
        'station',
        'commodities',
        'timestamp',
        'uploader',
        'software',
        'version',
        ])):
    pass
