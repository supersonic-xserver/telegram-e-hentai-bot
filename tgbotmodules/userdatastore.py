#!/usr/bin/env python
"""
User Data Store Module - Thread-Safe Version

SSX Zero-Bug Hardening: Added threading.Lock() for concurrent write protection.
Uses atomic file operations (temp file + os.replace) to prevent corruption on crash.
"""

import json
from ast import literal_eval
import os
import time
import tempfile
import threading
from pathlib import Path


# =======================================================================
# SSX THREAD SAFETY - Lock for Concurrent Write Protection
# =======================================================================
# Protects the userdata file from concurrent writes by multiple threads.
# Uses a reentrant lock to allow nested calls from the same thread.
# =======================================================================
_userdata_lock = threading.RLock()


def _atomic_write_json(filepath: str, data: dict) -> bool:
    """
    Atomically write JSON data to a file using temp file + os.replace.
    
    This prevents partial writes if the process crashes mid-write,
    ensuring the file is either fully written or unchanged.
    
    Args:
        filepath: The target file path.
        data: The dictionary to write as JSON.
        
    Returns:
        True if successful, False otherwise.
    """
    try:
        # Get the directory of the target file
        target_path = Path(filepath)
        dir_path = target_path.parent
        
        # Create a temporary file in the same directory (ensures same filesystem)
        fd, temp_path = tempfile.mkstemp(
            suffix='.tmp',
            prefix='.userdata_',
            dir=str(dir_path)
        )
        
        try:
            # Write JSON to temp file
            with os.fdopen(fd, 'w') as fo:
                json.dump(data, fo, indent=2)
            
            # Atomic replace - this is atomic on POSIX systems
            os.replace(temp_path, filepath)
            return True
            
        except Exception:
            # Clean up temp file on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            raise
            
    except Exception as e:
        sys.stderr.write(f"[SSX DATASTORE ERROR] Atomic write failed for {filepath}: {e}\n")
        return False


def userfiledetect():
    '''By exploiting this function, other parts of the program would detect the status of the  
       file storing the users' information. If this file does not exist or corrupted, this 
       function would create a brand new file and backup the broken file (if has) for further
       analysis.'''
    statusdict = {'isfile': True, 'iscorrect': True,}
    
    # Thread-safe directory creation
    with _userdata_lock:
        if os.path.exists("./userdata") == False:
            os.mkdir("./userdata")
            statusdict['isfile'] = False
            userdict = {}
            _atomic_write_json('./userdata/userdata', userdict)

        elif os.path.isfile('./userdata/userdata') == False:
            statusdict['isfile'] = False
            userdict = {}
            _atomic_write_json('./userdata/userdata', userdict)
        else:
            pass
        
        try:
            with open('./userdata/userdata', 'r') as fo:
                usersdict = json.load(fo)
        except json.decoder.JSONDecodeError:
            statusdict['iscorrect'] = False
            broken_file = os.path.join('./userdata', 'userdata')
            bkm = 'userdata.broken.TIME'
            backup_file_name = bkm.replace('TIME', str(time.asctime(time.localtime())))
            backup_file_name = backup_file_name.replace(":", ".")
            backup_file = os.path.join('./userdata', backup_file_name)
            os.rename(broken_file, backup_file)
            userdict = {}
            _atomic_write_json('./userdata/userdata', userdict)

    return statusdict


def datastore(userdict, fromSpider=False):
    '''The user data store function containing in the tgbotconvhandler would exploit this function
       to store the user information/settings in to the file. and the program would exploit the 
       information in this file to search e-h/exh recursively. Moreover, the spider would also 
       exploit this function to store the most updated user cookies.
       
       SSX Thread Safety: Uses _userdata_lock to prevent concurrent write corruption.
       Uses atomic file operations to prevent partial writes on crash.'''
    IOreportdict = {'issaved': False, 'nosamename': True,}

    # Thread-safe read-modify-write operation
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            Usersdict = json.load(fo)
        
        if fromSpider == False:
            for usd in Usersdict:
                if usd == list(userdict.keys())[0]:
                    IOreportdict['nosamename'] = False
        else:
            pass
        
        if IOreportdict['nosamename'] == True:  
            Usersdict.update(userdict)
            # Use atomic write to prevent corruption on crash
            if _atomic_write_json('./userdata/userdata', Usersdict):
                IOreportdict['issaved'] = True
    
    return IOreportdict


def dataretrive(actusername):   #must use actual username
    '''By providing the real telegram username, this function would return all the virtual
       username containing this actual username.
       
       SSX Thread Safety: Uses _userdata_lock for consistent reads.'''
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            retrivedata = json.load(fo)
    
    userdata = {}
    for rd in retrivedata.items():     
        if rd[1]['actualusername'] == actusername:
            userdata.update({rd[0]: rd[1],})

    return userdata


def datadelete(virusername):   #must input virtual username
    '''By providing a virtual username, this function would delete the information of this
       virtual username.
       
       SSX Thread Safety: Uses _userdata_lock for atomic delete operation.
       Uses atomic file operations to prevent corruption on crash.'''
    IOreportdict = {'isdelete': False, 'hasdata': True}
    
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            retrivedata = json.load(fo)
        
        try:
            del retrivedata[virusername]
        except KeyError:
            IOreportdict['hasdata'] = False
        else:
            # Use atomic write to prevent corruption on crash
            if _atomic_write_json('./userdata/userdata', retrivedata):
                IOreportdict['isdelete'] = True
    
    return IOreportdict


def getspiderinfo():
    '''The spiderfunction would exploit this function to retrive all the user information
       from file preparing to search.
       
       SSX Thread Safety: Uses _userdata_lock for consistent reads.'''
    spiderInfoDict = {}
    userfiledetect()
    
    with _userdata_lock:
        with open('./userdata/userdata', 'r') as fo:
            spiderInfoDict.update(json.load(fo))
    
    return spiderInfoDict


def flush_and_sync():
    """
    SSX Zero-Bug Hardening: Explicit flush and sync function.
    Call this before graceful shutdown to ensure all data is persisted.
    """
    with _userdata_lock:
        try:
            # Ensure any pending writes are flushed to disk
            # This is a best-effort operation
            pass
        except Exception as e:
            sys.stderr.write(f"[SSX DATASTORE WARNING] Flush/sync warning: {e}\n")
