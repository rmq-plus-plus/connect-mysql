# -*- coding: utf-8 -*-

from bottle import *
import json
import bottle

@bottle.route('/accept_binlog', method='POST')
def accept_binlog():
    data = bottle.request.body.readlines()
    print str(data)
    return 'hi, binlog'

run(host='0.0.0.0', port=8280, quiet=True)