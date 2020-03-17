# -*- coding: utf-8 -*-

# TESS UTILITY TO PERFORM SOME MAINTENANCE COMMANDS

# ----------------------------------------------------------------------
# Copyright (c) 2014 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import sys
import collections

# Python3 catch
try:
    raw_input
except:
    raw_input = input 

#--------------
# other imports
# -------------

import tabulate

#--------------
# local imports
# -------------

from .s4a import datetime

# ----------------
# Module constants
# ----------------

# ----------------
# package constants
# ----------------


# -----------------------
# Module global variables
# -----------------------


# --------------
# Module classes
# --------------


# -----------------------
# Module global functions
# -----------------------


def mkdate(datestr):
    date = none
    for fmt in ['%Y-%m','%Y-%m-%d','%Y-%m-%dT%H:%M:%S','%Y-%m-%dT%H:%M:%SZ']:
        try:
            date = datetime.strptime(datestr, fmt)
        except ValueError:
            pass
    return date


def utf8(s):
    if sys.version_info[0] < 3:
        return unicode(s, 'utf8')
    else:
        return (s)

def percent(n):
   n = max(n,0)
   n = min(n,100)
   return n

def shift_generator(iterable, N):
    '''Partitions a very long iterable in tuples of size N shifting one item at every ietration'''
    q = collections.deque((None for x in range(0,N)), N)
    for current in iterable:
        q.append(current)
        yield tuple(x for x in q)


if sys.version_info[0] < 3:
    # Python 2 version
    def packet_generator(iterable, size):
        '''Generates a sequence of 'size' items from an iterable'''
        finished = False
        while not finished:
            acc = []
            for i in range(0,size):
                try:
                    obj = iterable.next()
                except AttributeError:
                    iterable = iter(iterable)
                    obj = iterable.next()
                    acc.append(obj)
                except StopIteration:
                    finished = True
                    break
                else:
                    acc.append(obj)
            if len(acc):
                yield acc
else:
    # Python 3 version
    def packet_generator(iterable, size):
        '''Generates a sequence of 'size' items from an iterable'''
        finished = False
        while not finished:
            acc = []
            for i in range(0,size):
                try:
                    obj = iterable.__next__()
                except AttributeError:
                    iterable = iter(iterable)
                    obj = iterable.__next__()
                    acc.append(obj)
                except StopIteration:
                    finished = True
                    break
                else:
                    acc.append(obj)
            if len(acc):
                yield acc


def paging(iterable, headers, maxsize=10, page_size=10):
    '''
    Pages query output and displays in tabular format
    '''
    for rows in packet_generator(iterable, page_size):
        print(tabulate.tabulate(rows, headers=headers, tablefmt='grid'))
        maxsize -= page_size
        if len(rows) == page_size and maxsize > 0:
            raw_input("Press <Enter> to continue or [Ctrl-C] to abort ...")
        else:
            break

# ==============
# DATABASE STUFF
# ==============


def previous_iterable(connection, iterable):
    result = []
    for srcrow in iterable:
        row = {'rank': srcrow[0], 'name': srcrow[3]}
        cursor = connection.cursor()
        cursor.execute(
            '''
            SELECT r.rank, r.rejected, r.tstamp, r.name, r.sequence_number, r.frequency, r.magnitude, r.ambient_temperature, r.sky_temperature, r.signal_strength
            FROM raw_readings_t AS r
            WHERE r.rank == :rank - 1
            AND   r.name == :name
            AND   r.rejected IS NULL
            ''', row)
        temp = cursor.fetchone()
        if temp is not None:
            result.append(temp)
    return result
