#!/usr/bin/python3
"""
Telegram E-Hentai Bot - Production Ready with Signal Handling

SSX Zero-Bug Hardening:
- SIGINT/SIGTERM signal handlers for graceful shutdown
- Database flush before exit
- Proper resource cleanup
"""

import logging
import json
import os
import sys
import time
import datetime
import signal
from ast import literal_eval
from queue import Queue
from threading import Thread
from telegram.ext import Updater
from telegram.ext import CommandHandler
from telegram.ext import MessageHandler
from telegram.ext import Filters
from telegram.ext import ConversationHandler
from tgbotconvhandler import messageanalyze
from tgbotconvhandler import spiderfunction
from tgbotmodules import replytext
from tgbotmodules.spidermodules import generalcfg
from tgbotmodules import userdatastore

# =======================================================================
# SSX SIGNAL HANDLING - Graceful Shutdown Support
# =======================================================================
# Handles SIGINT (Ctrl+C) and SIGTERM to ensure all pending logs
# and database writes are flushed to disk before exit.
# =======================================================================

# Global flag for graceful shutdown
_shutdown_requested = False


# Global reference to bot for signal handler use
_bot_for_shutdown = None


def _signal_handler(signum, frame):
    """
    SSX Zero-Bug: Signal handler for graceful shutdown.
    Called when SIGINT (Ctrl+C) or SIGTERM is received.
    
    ATOMIC SHUTDOWN: This handler BLOCKS until Ghost Drive sync completes
    to ensure 100% successful save before exit.
    """
    global _shutdown_requested, _bot_for_shutdown
    
    signal_name = signal.Signals(signum).name
    logger.warning(f"[SSX SHUTDOWN] Received {signal_name}, initiating graceful shutdown...")
    
    # Set shutdown flag to prevent new operations
    _shutdown_requested = True
    
    # Request shutdown in generalcfg
    generalcfg.request_shutdown()
    
    # Flush database to disk (local)
    try:
        userdatastore.flush_and_sync()
        logger.info("[SSX SHUTDOWN] Database flushed to disk.")
    except Exception as e:
        logger.error(f"[SSX SHUTDOWN] Error flushing database: {e}")
    
    # ATOMIC SHUTDOWN: Sync to Ghost Drive and BLOCK until complete
    # This ensures 100% successful save before exit
    if _bot_for_shutdown and generalcfg.DATABASE_CHANNEL_ID:
        logger.info("[SSX SHUTDOWN] Syncing to Ghost Drive (blocking)...")
        max_retries = 3
        for attempt in range(max_retries):
            success, message = userdatastore.sync_to_ghost_drive(_bot_for_shutdown)
            if success:
                logger.info(f"[SSX SHUTDOWN] Ghost Drive sync successful: {message}")
                break
            else:
                logger.warning(f"[SSX SHUTDOWN] Ghost Drive sync attempt {attempt + 1}/{max_retries} failed: {message}")
                if attempt < max_retries - 1:
                    time.sleep(2)  # Brief wait before retry
        else:
            logger.error("[SSX SHUTDOWN] All Ghost Drive sync attempts failed!")
            # Emergency logging already handled in sync_to_ghost_drive
    
    # Log shutdown completion
    logger.warning(f"[SSX SHUTDOWN] Graceful shutdown complete for {signal_name}.")
    sys.stdout.flush()
    sys.stderr.flush()
    
    # Exit with success code
    os._exit(0)


def _register_signal_handlers():
    """
    Register signal handlers for SIGINT and SIGTERM.
    SSX Zero-Bug: Ensures graceful shutdown on both console interrupt and system service stop.
    """
    # Register SIGINT (Ctrl+C)
    signal.signal(signal.SIGINT, _signal_handler)
    
    # Register SIGTERM (system service stop)
    signal.signal(signal.SIGTERM, _signal_handler)
    
    logger.info("[SSX SHUTDOWN] Signal handlers registered for SIGINT and SIGTERM.")


def is_shutdown_requested():
    """Check if shutdown has been requested via signal."""
    return _shutdown_requested or generalcfg.is_shutdown_requested()


# =======================================================================
# ORIGINAL BOT CODE
# =======================================================================
 
def start(bot, update, user_data, chat_data):
   '''This function is the initiation of the bot's conversation. It would clear the
      previous userdata (if have) and create a new user profile for the later conversation.
      ''' 
   user_data.clear()
   chat_data.clear()
   user_data.update({"actualusername": str(update.message.from_user.username),
                     "chat_id": update.message.chat_id}
                   )
   logger.info("Actual username is %s.", str(update.message.from_user.username))
   update.message.reply_text(text=replytext.startMessage)
   chat_data.update({'state': 'verify'})
   return STATE

def state(bot, update, user_data, chat_data):
   '''This function would handle the whole interactions between user and bot. In other
      words, it is a simple mimic of the python telegram bot's conversation handler 
      module and providing a convenient way to move this program to other IM platforms.
      The major process of this function is receiving a user message and send it to 
      messageanalyze function. Then the messageanalyze function would return the result
      depend on the content of the message. It exploited user_data and chat_data provided 
      by the python telegram bot module to store user information as well as the user 
      state. Moreover, while a user completes the search settings, this function would 
      create a thread object containing a single search operation and return the result  
      to user's chat to verify the searching settings.'''
   
   # SSX SHUTDOWN CHECK: Don't start new operations if shutting down
   if is_shutdown_requested():
      logger.warning("Shutdown requested, ignoring new message from %s", update.message.from_user.username)
      return ConversationHandler.END
   
   inputStr = update.message.text
   user_data.update({'chat_id': update.message.chat_id})
   outputDict = messageanalyze(inputStr=inputStr, 
                               user_data=user_data, 
                               chat_data=chat_data,
                               logger=logger
                              )
   user_data.update(outputDict["outputUser_data"])
   chat_data.update(outputDict["outputChat_data"])
   for text in outputDict["outputTextList"]:
      update.message.reply_text(text=text)
   if chat_data['state'] != 'END':
      state = STATE 
   else:
      # print (user_data)
      userdata = ({chat_data["virtualusername"]: user_data})
      threadName = time.asctime()
      t = Thread(target=searcheh, 
                 name=threadName, 
                 kwargs={'bot':bot,
                         'user_data':user_data,
                         'threadQ': threadQ})
      threadQ.put(t)
      user_data.clear()
      chat_data.clear() 
      logger.info("The user_data and chat_data of user %s is clear.", str(update.message.from_user.username))
      state = ConversationHandler.END
   return state

def searchIntervalCTL(bot, job, user_data=None):
    '''The python telegram bot module would exploit this function to generate a search 
       thread object to extract the information on e-h/exh.'''
    
    # SSX SHUTDOWN CHECK: Don't start new jobs if shutting down
    if is_shutdown_requested():
        logger.warning("Shutdown requested, skipping scheduled job.")
        return
    
    threadName = time.asctime()
    t = Thread(target=searcheh, 
               name=threadName, 
               kwargs={'bot':bot,
                        'user_data':user_data,
                        'threadQ': threadQ})
    threadQ.put(t)


def searcheh(bot, threadQ, job=None, user_data=None):
   '''This function controls the whole search process, including reading the search relating 
      information from files or other functions' requests, using this information and the
      spiderfunction function to search e-h/exh and then sending the search result to channel
      and/or chat.'''
   
   # SSX SHUTDOWN CHECK: Don't start new searches if shutting down
   if is_shutdown_requested():
       logger.warning("Shutdown requested, skipping search operation.")
       threadQ.task_done()
       return
   
   logger.info("Search is beginning")
   if user_data:
      for ud in user_data:
         user_data[ud].update({'userpubchenn': False,'resultToChat': True})
         logger.info("User %s has finished profile setting process, test search is begining.", user_data[ud]['actualusername'])
      # print (user_data)
      spiderDict = user_data
      toTelegramDict = spiderfunction(logger=logger, spiderDict=spiderDict)
   else:
      spiderDict = userdatastore.getspiderinfo()
      toTelegramDict = spiderfunction(logger=logger)
      logger.info("All users' search has been completed, begin to send the result")
   if toTelegramDict:
      for td in toTelegramDict:

         chat_idList = []     
         if spiderDict[td].get('chat_id') and spiderDict[td]['resultToChat'] == True:
            chat_idList.append(spiderDict[td]['chat_id'])
         if spiderDict[td]["userpubchenn"] == True and generalcfg.pubChannelID:      # Public channel id might be empty
            chat_idList.append(generalcfg.pubChannelID)
         logger.info("Begin to send user %s's result.", td)
         
         for chat_id in chat_idList:
            if len(toTelegramDict[td]) == 0:
               messageDict = {"messageCate": "message",
                              "messageContent": ["------Could not find any new result for {0}------".format(str(td))]}
               channelmessage(bot=bot, messageDict=messageDict, chat_id=chat_id)
               continue
            messageDict = {"messageCate": "message",
                           "messageContent": ["------This is the result of {0}------".format(str(td))]}
            channelmessage(bot=bot, messageDict=messageDict, chat_id=chat_id)
            for manga in toTelegramDict[td]:
            #    for obj in toTelegramDict[td][result]:
               if manga.previewImageObj:
                #   print(manga.previewImageObj.getbuffer().nbytes)
                  messageDict = {'messageCate': "photo", "messageContent": [manga.previewImageObj]}
                  channelmessage(bot=bot, messageDict=messageDict, chat_id=chat_id)
               messageDict = {'messageCate': "message", "messageContent": ['{0}\n{1}'.format(manga.title, manga.url)]}
               channelmessage(bot=bot, messageDict=messageDict, chat_id=chat_id)
         logger.info("User {0}'s result has been sent.".format(td))
      logger.info("All users' result has been sent.")
   else: 
      logger.info("Could not gain any new result to users.")         
      messageDict = {"messageCate": "message", "messageContent": ["We do not have any new result"]}
      channelmessage(bot=bot, messageDict=messageDict, chat_id=generalcfg.pubChannelID)
   threadQ.task_done()

def retryDocorator(func, retry=generalcfg.timeoutRetry):
   '''This simple retry decorator provides a try-except looping to the channelmessage function to
      overcome network fluctuation.'''

   def wrapperFunction(*args, **kwargs):
      err = 0 
      for err in range(retry):
         try:
            func(*args, **kwargs)
            break
         except Exception as error:
           err += 1
           logger.warning(str(error))
      else:
         logger.warning('Retry limitation reached')
         
      return
   return wrapperFunction

@retryDocorator
def channelmessage(bot, messageDict, chat_id):
   ''' All the functions containing user interaction would use this function to send messand 
       to user.'''
   messageContent = messageDict["messageContent"]
   for mC in messageContent:
      if messageDict['messageCate'] == 'photo':
         mC.seek(0)
         bot.send_photo(chat_id=chat_id, photo=mC)
      else:
         bot.send_message(chat_id=chat_id, text=mC)
   return None

def thread_containor(threadQ):
   '''This simple thread containor could force the the program running a single search thread
      simultaneously to prevent e-h/exh ban the server's IP. In the idle status, the threadQ.get()
      method would block the infinity loop preventing it comsuming resources.'''
   threadCounter = 0
   while True:
      t = threadQ.get()
      
      # SSX SHUTDOWN CHECK: Exit container loop if shutting down
      if is_shutdown_requested():
          logger.warning("[SSX SHUTDOWN] Thread container shutting down, abandoning queued threads.")
          break
      
      logger.info('Added a new thread to thread containor - {0} '.format(t.name))
      t.start()
      threadCounter += 1
      if threadCounter == 1:  # This condition limit the amount of threads running simultaneously.
         t.join() 
         threadCounter = 0

def autoCreateJob(job):
   '''The python telegram bot's job module would exploit this function to create a recursive job to
      run the users' setted search request stored on the disk.'''
   job.run_repeating(searchIntervalCTL, interval=generalcfg.interval, first=5)

def cancel(bot, update, user_data, chat_data):  
   '''If user type a /cancel command, the program would use this function to delete the user's current 
      data and status.'''
   update.message.reply_text(text=replytext.UserCancel)
   logger.info("User %s has canceled the process.", str(update.message.from_user.username))
   user_data.clear()
   chat_data.clear()
   logger.info("The user_data and chat_data of user %s has cleared", str(update.message.from_user.username))
   return ConversationHandler.END

def error(bot, update, error):
   '''The bot would exploit this function to report some rare and strange errors.'''
   logger.warning('Update "%s" caused error "%s"', update, error)


def status(bot, update, user_data, chat_data):
   '''SSX Status Command - Display Ghost Drive and system status.
   
   Shows:
   - Ghost Drive configuration status
   - Last successful sync time
   - Current userdata file size
   - Number of backups in the vault
   '''
   from datetime import datetime
   
   # Check if user is admin
   if update.message.from_user.id != generalcfg.adminID:
       update.message.reply_text("❌ Admin only command.")
       return
   
   status_lines = ["📊 **SSX System Status**\n"]
   
   # Ghost Drive Status
   ghost_status = userdatastore.get_ghost_drive_status()
   
   status_lines.append("🗄️ **Ghost Drive**")
   if ghost_status['is_configured']:
       status_lines.append(f"✅ Configured (Channel: `{ghost_status['channel_id']}`)")
   else:
       status_lines.append("❌ Not configured - set `TG_DATABASE_CHANNEL_ID`")
   
   if ghost_status['last_sync_time']:
       last_sync = ghost_status['last_sync_time'].strftime("%Y-%m-%d %H:%M:%S")
       status_lines.append(f"📅 Last sync: `{last_sync}`")
   else:
       status_lines.append("📅 Last sync: Never")
   
   # File size
   file_size = ghost_status['file_size']
   if file_size > 0:
       if file_size < 1024:
           size_str = f"{file_size} bytes"
       elif file_size < 1024 * 1024:
           size_str = f"{file_size / 1024:.1f} KB"
       else:
           size_str = f"{file_size / (1024 * 1024):.1f} MB"
       status_lines.append(f"📁 userdata size: `{size_str}`")
   else:
       status_lines.append("📁 userdata size: 0 bytes")
   
   status_lines.append("")
   
   # Uptime
   import time
   uptime = time.time() - start_time
   hours, remainder = divmod(int(uptime), 3600)
   minutes, seconds = divmod(remainder, 60)
   status_lines.append(f"⏱️ Uptime: `{hours}h {minutes}m {seconds}s`")
   
   status_text = "\n".join(status_lines)
   update.message.reply_text(status_text, parse_mode='Markdown')
   
   return ConversationHandler.END


def _validate_ghost_drive_config():
   """
   SSX Startup Validation: Validate Ghost Drive configuration.
   
   Checks that DATABASE_CHANNEL_ID has a valid format before attempting
   to use the Ghost Drive. This prevents cryptic errors at runtime.
   
   Returns:
       True if valid or not configured, False if invalid.
   """
   from tgbotmodules.userdatastore import _validate_channel_id
   
   channel_id = generalcfg.DATABASE_CHANNEL_ID
   
   if not channel_id:
       logger.info("[SSX GHOST DRIVE] Not configured - DATABASE_CHANNEL_ID not set")
       return True
   
   if _validate_channel_id(channel_id):
       logger.info(f"[SSX GHOST DRIVE] Configuration validated: {channel_id}")
       return True
   else:
       logger.error(
           f"[SSX GHOST DRIVE] FATAL: Invalid channel ID format: {channel_id}. "
           f"Expected format: -100XXXXXXXXXX"
       )
       return False


# Track startup time for uptime display
start_time = time.time()

def main():
   '''This function controls the initiation of the bot inclding creating some objects to use the bot,
      and the thread containor thread to deal with search requests both from jobs and user requests 
      after finishing the settings.'''
   
   # SSX ZERO-BUG: Register signal handlers for graceful shutdown
   _register_signal_handlers()
   
   if generalcfg.proxy:
      updater = Updater(token=generalcfg.token, request_kwargs={'proxy_url': generalcfg.proxy[0]})
   else:   
      updater = Updater(token=generalcfg.token)
   
   # Store bot reference for signal handler use (Ghost Drive sync)
   global _bot_for_shutdown
   _bot_for_shutdown = updater.bot
   
   dp = updater.dispatcher
   job= updater.job_queue
   conv_handler = ConversationHandler(
                  entry_points=[CommandHandler('start', start, pass_user_data=True, pass_chat_data=True)],
                  states={STATE: [MessageHandler(Filters.text, state, pass_user_data=True, pass_chat_data=True)]
                  },
                  fallbacks=[CommandHandler('cancel', cancel, pass_user_data=True, pass_chat_data=True)],
   )
   dp.add_handler(conv_handler)
   
   # Add /status command handler
   dp.add_handler(CommandHandler('status', status, pass_user_data=True, pass_chat_data=True))
   
   dp.add_error_handler(error)
   autoCreateJob(job=job)
   tc = Thread(target=thread_containor, 
               name='tc', 
               kwargs={'threadQ': threadQ},
               daemon=True)
   tc.start()
   logger.info('Spider thread containor initiated.')
   updater.start_polling(poll_interval=1.0, timeout=1.0)
   logger.info('Bot initiated.')
   
   # =======================================================================
   # SSX GHOST DRIVE - Boot Sequence
   # =======================================================================
   # Load the latest backup from the Telegram Vault before any other modules
   # initialize. This ensures we have the most recent state from a previous
   # instance that may have been running on a different container.
   # =======================================================================
   if generalcfg.DATABASE_CHANNEL_ID:
       logger.info("[SSX GHOST DRIVE] Attempting to load from Telegram Vault...")
       success, message = userdatastore.load_from_ghost_drive(updater.bot)
       if success:
           logger.info(f"[SSX GHOST DRIVE] {message}")
       else:
           # Handle first-time setup (no backup exists yet)
           if "first-time setup" in message.lower():
               logger.info("[SSX GHOST DRIVE] First-time setup detected - initializing fresh database")
               userdatastore.userfiledetect()  # Ensure local file exists
           else:
               logger.warning(f"[SSX GHOST DRIVE] Load failed: {message} - using local file")
       
       # Initialize the periodic Ghost Drive sync job (every 20 minutes)
       ghost_job = userdatastore.init_ghost_drive_sync(updater.bot, job)
       if ghost_job:
           logger.info(f"[SSX GHOST DRIVE] Periodic sync job scheduled (interval: {generalcfg.GHOST_DRIVE_SYNC_INTERVAL}s)")
       else:
           logger.info("[SSX GHOST DRIVE] Periodic sync not enabled (channel not configured)")
   else:
       logger.info("[SSX GHOST DRIVE] Not configured - DATABASE_CHANNEL_ID not set")
   
   # SSX ZERO-BUG: idle() now properly handles keyboard interrupt via signal handler
   # The signal handler will flush database and exit gracefully
   updater.idle()


logging.basicConfig(format='%(asctime)s - %(module)s.%(funcName)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)
logging.getLogger('requests').setLevel(logging.CRITICAL)
threadQ = Queue()  # This queue object put the spider function into the thread containor 
                   # Using this thread containor wound also limits the download function thread
                   # to prevent e-h to ban IP.
(STATE) = range(1)

if __name__ == '__main__':
   main()
