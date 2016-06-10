#!/usr/bin/env python

from app import app
# command to start flask server: sudo python run.py
app.run(host="0.0.0.0", port=80, debug = True)
