#!/usr/bin/env python

"""
@package ion.services.mi.instrument_protocol Base instrument protocol structure
@file ion/services/mi/instrument_protocol.py
@author Steve Foley
@brief Instrument protocol classes that provide structure towards the
nitty-gritty interaction with individual instruments in the system.
@todo Figure out what gets thrown on errors
"""

__author__ = 'Steve Foley'
__license__ = 'Apache 2.0'

from ion.services.mi.exceptions import InstrumentProtocolException
from ion.services.mi.exceptions import InstrumentTimeoutException
from ion.services.mi.exceptions import InstrumentStateException

class InstrumentProtocol (Object):
    '''The base class for an instrument protocol
    
    The classes derived from this class will carry out the specific
    interactions between a specific device and the instrument driver. At this
    layer of interaction, there are no conflicts or transactions as that is
    handled at the layer above this. Think of this as encapsulating the
    transport layer of the communications.
    '''
    
    '''List of possible commands supported by the protocol'''
    self.instrument_commands = None
    
    '''List of possible parameters supported by the protocol'''
    self.instrument_parameters = None
    
    '''List of status parameters supported by the protocol'''
    self.instrument_status_param = None
    
    def __init__(self):
        pass
        # Fill in instance vars of commands and parameters from
        # instrument-specific driver parameters
        
    def get(self, params=[]):
        '''Get some parameters
        
        @param params A list of parameters to fetch. These must be in the
        fetchable parameter list
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        '''
        assert(isinstance(params, list))

    def set(self, params={}):
        '''Get some parameters
        
        @param params A dict with the parameters to fetch. Must be in the
        fetchable list
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        '''
        assert(isinstance(params, dict))

    def execute(self, command=[]):
        '''Execute a command
        
        @param command A single command as a list with the command ID followed
        by the parameters for that command
        @throws InstrumentProtocolException Confusion dealing with the
        physical device
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        '''
        assert(isinstance(command, list))
        
    def get_config(self):
        '''Get an entire configuration from a device
        
        @retval config A dict with all of the device's parameters and values at a
        given moment in time.
        @throws InstrumentProtocolException Confusion dealing with the
        physical device, possibly due to interrupted communications
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        '''
    
    def restore_config(self, config={}):
        '''Restore the complete supplied config to the device.
        
        This method must take into account any ordering of set requests as
        required to make the entire operation stick. Should have the ability
        to back out changes that failed mid-application, too.
        
        @param config A dict structure of the configuration that should be
        applied to the instrument. May have come directly from a call to
        get_config at some point before.
        @throws InstrumentProtocolException Confusion dealing with the
        physical device, possibly due to interrupted communications
        @throws InstrumentStateException Unable to handle current or future
        state properly
        @throws InstrumentTimeoutException Timeout
        '''
        assert(isinstance(config, dict))
    
    def get_status(self):
        '''Gets the current status of the instrument.
        
        @retval status A dict of the current status of the instrument. Keys are
        listed in the status parameter list.
        '''
    
class BinaryInstrumentProtocol(InstrumentProtocol):
    '''Instrument protocol description for a binary-based instrument
    
    This class wraps standard protocol operations with methods to pack
    commands into the binary structures that they need to be in for the
    instrument to operate on them.
    @todo Consider removing this class if insufficient parameterization of
    message packing is done
    '''
    
    def _pack_msg(self, msg=None):
        '''Pack the message according to the field before sending it to the
        instrument.
        
        This may involve special packing per parameter, possibly checksumming
        across the whole message, too.
        @param msg The message to pack for the instrument. May need to be
        inspected to determine the proper packing structure
        @retval packed_msg The packed message
        '''
        # Default implementation
        return msg.checksum
    
    def _unpack_response(self, type=None, packed_msg=None):
        '''Unpack a message from an instrument
        
        When a binary instrument responsed with a packed binary, this routine
        unbundles the response into something usable. Checksums may be added
        @param type The type of message to be unpacked. Will like be a key to
        an unpacking description string.
        @param packed_msg The packed message needing to be unpacked
        @retval msg The unpacked message
        '''
        # Default implementation
        return packed_msg
    
    
class CommandResponseInstrumentProtocol(InstrumentProtocol):
    '''A baseclass for text-based command/response instruments
    
    For instruments that have simple command and response interations, this
    class provides some structure for manipulating data to and from the
    instrument.
    '''
    
    '''The command keys to be used'''
    self.command_list = None
    
    '''The response regex dict to be used to map a command's repsonse to
    a specific format
    '''
    self.response_regex_list = None
    
    '''The separater string between the name and value when sending a
    command. For example, a 'name = value' command would have ' = '
    '''
    self.send_name_value_delimiter = ""

    '''The separater string between the name and value when receiving a
    command. For example, a 'name = value' command would have ' = '
    '''
    self.receive_name_value_delimiter = ""

    '''The end-of-line delimiter to use'''
    self.eoln = None
    
    def __init__(self):
        # Initialize instance variables
        pass
    
    def _identify_response(self, response_str=""):
        '''Format the response to a command into a usable form
        
        @param response_str The raw response string from the instrument
        @retval response A usably-formatted response structure. A dict?
        '''
        assert(instanceof(response_str, string))
        # Apply regexes, separators, delimiters, Eolns, etc.
        
    def _build_command(self, command=None):
        '''Construct a command string based on the values supplied
        
        @param command A usable struucture (dict?) that needs to be converted
        into a string for the instrument
        @retval command_str The string to send to the instrument
        '''
        assert(instanceof(command, dict))
        # Apply regexes, separators, delimiters, Eolns, etc.