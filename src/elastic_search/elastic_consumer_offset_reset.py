#!/usr/bin/env python3


"""
What do we do when the consumer is down past the number of the days that kafka can hold the data/offsets for?
Consumer offset can be lost if a consumer hasn't read new data in 1 day (kafka < 2.0), if a consumer hasn't read
new data in 7 days (kafka >= 2.0)

This can be controlled by the broker setting ====>>>>> offset.retention.minutes
"""
