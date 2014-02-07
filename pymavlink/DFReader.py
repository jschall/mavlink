#!/usr/bin/env python
'''
APM DataFlash log file reader

Copyright Andrew Tridgell 2011
Released under GNU GPL version 3 or later

Partly based on SDLog2Parser by Anton Babushkin
'''

import struct, time, os
from pymavlink import mavutil
from collections import deque
from datetime import datetime

def formattime(time):
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

FORMAT_TO_STRUCT = {
    "b": ("b", None, int),
    "B": ("B", None, int),
    "h": ("h", None, int),
    "H": ("H", None, int),
    "i": ("i", None, int),
    "I": ("I", None, int),
    "f": ("f", None, float),
    "n": ("4s", None, str),
    "N": ("16s", None, str),
    "Z": ("64s", None, str),
    "c": ("h", 0.01, float),
    "C": ("H", 0.01, float),
    "e": ("i", 0.01, float),
    "E": ("I", 0.01, float),
    "L": ("i", 1.0e-7, float),
    "M": ("b", None, int),
    "q": ("q", None, int),
    "Q": ("Q", None, int),
    }

class DFFormat(object):
    def __init__(self, name, len, format, columns):
        self.name = name
        self.len = len
        self.format = format
        self.columns = columns.split(',')

        msg_struct = "<"
        msg_mults = []
        msg_types = []
        for c in format:
            if ord(c) == 0:
                break
            try:
                (s, mul, type) = FORMAT_TO_STRUCT[c]
                msg_struct += s
                msg_mults.append(mul)
                msg_types.append(type)
            except KeyError as e:
                raise Exception("Unsupported format char: '%s' in message %s" % (c, name))

        self.msg_struct = msg_struct
        self.msg_types = msg_types
        self.msg_mults = msg_mults

def null_term(str):
    '''null terminate a string'''
    idx = str.find("\0")
    if idx != -1:
        str = str[:idx]
    return str

class DFMessage(object):
    def __init__(self, fmt, elements, apply_multiplier):
        self._d = {}
        self.fmt = fmt
        for i in range(len(fmt.columns)):
            mul = fmt.msg_mults[i]
            name = fmt.columns[i]
            self._d[name] = elements[i]
            if fmt.format[i] != 'M' or apply_multiplier:
                self._d[name] = fmt.msg_types[i](self._d[name])
            if fmt.msg_types[i] == str:
                self._d[name] = self._d[name]
                self._d[name] = null_term(self._d[name])
            if mul is not None and apply_multiplier:
                self._d[name] = self._d[name] * mul
        self._fieldnames = list(fmt.columns)
        self.__dict__.update(self._d)

    def add_value(self, name, value):
        self._d[name] = value
        self._fieldnames.append(name)
        self.__dict__.update(self._d)
    
    def get_type(self):
        return self.fmt.name

    def __str__(self):
        ret = "%s {" % self.fmt.name
        for c in self._fieldnames:
            ret += "%s : %s, " % (c, self._d[c])
        ret = ret[:-2] + "}"
        return ret

class DFReader(object):
    '''parse a generic dataflash file'''
    def __init__(self):
        # read the whole file into memory for simplicity
        self.msg_periods = {}
    
    def param(self, name, default=None):
        '''convenient function for returning an arbitrary MAVLink
        parameter with a default'''
        if not name in self.params:
            return default
        return self.params[name]
    
    def recv_match(self, condition=None, type=None, blocking=False):
        '''recv the next message that matches the given condition
        type can be a string or a list of strings'''
        if type is not None and not isinstance(type, list):
            type = [type]
        while True:
            m = self.recv_msg()
            if m is None:
                return None
            if type is not None and not m.get_type() in type:
                continue
            if not mavutil.evaluate_condition(condition, self.messages):
                continue
            return m
    
    def check_condition(self, condition):
        '''check if a condition is true'''
        return mavutil.evaluate_condition(condition, self.messages)
    
    def recv_msg(self):
        '''recv the next message'''
        if len(self.queue) == 0:
            self._fill_msg_queue()
        if len(self.queue) ==0:
            return None
        m = self.queue.pop()
        self._update_state(m)
        return m
    
    def _rewind(self):
        '''reset log state on rewind'''
        self.params = {}
        self.timestamp = 0
        self.messages = { 'MAV' : self }
        self.flightmode = "UNKNOWN"
        self.percent = 0
        self.queue = []
        self.last_gps_time = None
    
    def _get_gps_time(self, gps):
        '''retrieve GPS time from a GPS message'''
        time = None
        if 'T' in gps._fieldnames:
            return gps.T*0.001
        if 'Time' in gps._fieldnames:
            time = gps.Time*0.001
        if 'TimeMS' in gps._fieldnames:
            time = gps.TimeMS*0.001
        if 'Week' in gps._fieldnames:
            epoch = 86400*(10*365 + (1980-1969)/4 + 1 + 6 - 2)
            time = epoch + 86400*7*gps.Week + time - 15
        
        return time
    
    def _fill_msg_queue(self):
        if len(self.queue):
            return
        
        counts = {}
        
        while True:
            m = self._parse_next()
            m_type = None
            gps_time = None
            
            if m is not None:
                m_type = m.get_type()
                counts[m_type] = counts.get(m_type, 0) + 1
                self.queue.append(m)
            else:
                while len(self.queue) and self.queue.pop().get_type() != 'GPS': #just delete all messages after last GPS with time
                    continue
            
            if len(self.queue) == 0:
                return 0
            
            if m_type == 'GPS' and self._get_gps_time(m): #handle first GPS message
                gps_time = self._get_gps_time(m)
                #set last gps time and continue, we have to get at least one more GPS message to interpolate
                if self.last_gps_time is None:
                    self.last_gps_time = self._get_gps_time(m) 
                    continue
            
            
            if gps_time is not None or m is None: #hit a valid GPS message or end of log
                countdown = counts.copy()
                gps_time_delta = (gps_time - self.last_gps_time) * counts[m_type]
                self.last_gps_time = gps_time
                
                #fill in message times based on last GPS message time and number of messages of type
                for i in self.queue:
                    i_type = i.get_type()
                    
                    if i_type == 'GPS' or i_type == 'GPS2':
                        i._timestamp = self._get_gps_time(i)
                        if i._timestamp:
                            continue
                    
                    if 'T' in i._fieldnames:
                        i._timestamp = i.T
                        continue
                    
                    self.msg_periods[i_type] = gps_time_delta/counts[i_type]
                    i._timestamp = gps_time - self.msg_periods[i_type] * countdown[i_type] + self.msg_periods[i_type] * 0.5
                    
                    countdown[i_type] = countdown[i_type] - 1
                self.queue.sort(reverse=True, key=lambda x: x._timestamp) #sorted in reverse so that pop() will dequeue
                break
        return len(self.queue)
    
    def _update_state(self, m):
        '''add a new message'''
        if m is None:
            return
        type = m.get_type()
        
        self.messages[type] = m

        if type == 'PARM':
            self.params[m.Name] = m.Value
        if type == 'MODE':
            if isinstance(m.Mode, str):
                self.flightmode = m.Mode.upper()
            elif 'ModeNum' in m._fieldnames:
                self.flightmode = mavutil.mode_string_apm(m.ModeNum)
            else:
                self.flightmode = mavutil.mode_string_acm(m.Mode)

class DFReader_binary(DFReader):
    '''parse a binary dataflash file'''
    def __init__(self, filename):
        DFReader.__init__(self)
        # read the whole file into memory for simplicity
        f = open(filename, mode='rb')
        self.data = f.read()
        f.close()
        self.HEAD1 = 0xA3
        self.HEAD2 = 0x95
        self.formats = {
            0x80 : DFFormat('FMT', 89, 'BBnNZ', "Type,Length,Name,Format,Columns")
        }
        self._rewind()

    def _rewind(self):
        '''rewind to start of log'''
        DFReader._rewind(self)
        self.offset = 0
        self.remaining = len(self.data)

    def _parse_next(self):
        '''read one message, returning it as an object'''
        if len(self.data) - self.offset < 3:
            return None
            
        hdr = self.data[self.offset:self.offset+3]
        if (ord(hdr[0]) != self.HEAD1 or ord(hdr[1]) != self.HEAD2):
            return None
        msg_type = ord(hdr[2])

        self.offset += 3
        self.remaining -= 3

        if not msg_type in self.formats:
            raise Exception("Unknown message type %02x" % msg_type)
        fmt = self.formats[msg_type]
        if self.remaining < fmt.len-3:
            # out of data - can often happen half way through a message
            return None
        body = self.data[self.offset:self.offset+(fmt.len-3)]
        try:
            elements = list(struct.unpack(fmt.msg_struct, body))
        except Exception:
            print("Failed to parse %s/%s with len %u (remaining %u)" % (fmt.name, fmt.msg_struct, len(body), self.remaining))
            raise
        name = null_term(fmt.name)
        if name == 'FMT' and elements[0] not in self.formats:
            # add to formats
            # name, len, format, headings
            self.formats[elements[0]] = DFFormat(null_term(elements[2]), elements[1],
                                                 null_term(elements[3]), null_term(elements[4]))

        self.offset += fmt.len-3
        self.remaining -= fmt.len-3
        m = DFMessage(fmt, elements, True)

        self.percent = 100.0 * (self.offset / float(len(self.data)))
        
        return m

def DFReader_is_text_log(filename):
    '''return True if a file appears to be a valid text log'''
    f = open(filename)
    ret = (f.read(8000).find('FMT, ') != -1)
    f.close()
    return ret

class DFReader_text(DFReader):
    '''parse a text dataflash file'''
    def __init__(self, filename):
        DFReader.__init__(self)
        # read the whole file into memory for simplicity
        f = open(filename, mode='r')
        self.lines = f.readlines()
        f.close()
        self.formats = {
            'FMT' : DFFormat('FMT', 89, 'BBnNZ', "Type,Length,Name,Format,Columns")
        }
        self._rewind()

    def _rewind(self):
        '''rewind to start of log'''
        DFReader._rewind(self)
        self.line = 0
        # find the first valid line
        while self.line < len(self.lines):
            if self.lines[self.line].startswith("FMT, "):
                break
            self.line += 1

    def _parse_next(self):
        '''read one message, returning it as an object'''
        while self.line < len(self.lines):
            s = self.lines[self.line].rstrip()
            elements = s.split(", ")
            # move to next line
            self.line += 1
            if len(elements) >= 2:
                break

        # cope with empty structures
        if len(elements) == 5 and elements[-1] == ',':
            elements[-1] = ''
            elements.append('')

        self.percent = 100.0 * (self.line / float(len(self.lines)))

        if self.line >= len(self.lines):
            return None

        msg_type = elements[0]

        if not msg_type in self.formats:
            return None
        
        fmt = self.formats[msg_type]

        if len(elements) < len(fmt.format)+1:
            # not enough columns
            return None

        elements = elements[1:]
        
        name = fmt.name.rstrip('\0')
        if name == 'FMT':
            # add to formats
            # name, len, format, headings
            self.formats[elements[2]] = DFFormat(elements[2], int(elements[1]), elements[3], elements[4])

        m = DFMessage(fmt, elements, False)
        m.add_value("line", self.line)

        return m

if __name__ == "__main__":
    import sys
    filename = sys.argv[1]
    if filename.endswith('.log'):
        log = DFReader_text(filename)
    else:
        log = DFReader_binary(filename)
    while True:
        m = log.recv_msg()
        if m is None:
            break
        print m
