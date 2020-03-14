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


def tuple_generator(iterable, N):
    '''Partitions a very long iterable in tuples of size N'''
    q = collections.deque((None for x in range(0,N)), N)
    for current in iterable:
        q.append(current)
        yield tuple(x for x in q)

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

# ==============
# DATABASE STUFF
# ==============
 

def paging(cursor, headers, size=10):
    '''
    Pages query output and displays in tabular format
    '''
    ONE_PAGE = 10
    while True:
        result = cursor.fetchmany(ONE_PAGE)
        print(tabulate.tabulate(result, headers=headers, tablefmt='grid'))
        if len(result) < ONE_PAGE:
            break
        size -= ONE_PAGE
        if size > 0:
            raw_input("Press Enter to continue [Ctrl-C to abort] ...")
        else:
            break

def paging(iterable, headers, size=10):
    '''
    Pages query output and displays in tabular format
    '''
    ONE_PAGE = 10
    for rows in packet_generator(iterable, ONE_PAGE):
        print(tabulate.tabulate(rows, headers=headers, tablefmt='grid'))
        if len(rows) == ONE_PAGE:
            raw_input("Press Enter to continue [Ctrl-C to abort] ...")
    