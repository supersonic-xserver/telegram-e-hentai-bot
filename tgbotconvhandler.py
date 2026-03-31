#!/usr/bin/python3
import os
import json
import logging
import time
from ast import literal_eval
from tgbotmodules import userdatastore
from tgbotmodules import replytext
from tgbotmodules.spidermodules import generator # use the sleep function
from tgbotmodules import exhspider
from tgbotmodules import userdatastore
from tgbotmodules.spidermodules import generalcfg
from tgbotmodules import searchoptgen 
from io import BytesIO 

def verify(inputStr, user_data, chat_data, logger, context=None):
   outputTextList = []
   stored_passcode = generalcfg.passcode
   user_input = inputStr.strip() if inputStr else ""
   
   # ===================================================================
   # TYPE SHIELD - CRITICAL SECURITY CHECK (NUCLEAR RESET)
   # If user_data is NOT a dict (e.g., corrupted string), this prevents:
   # - "string indices must be integers" errors
   # - Authentication bypass via malformed data
   # - Dispatcher crashes from garbage entries
   # FIX: Clear ALL context state to prevent corrupted data from reaching dispatcher
   # ===================================================================
   if not isinstance(user_data, dict):
      logger.error(f"[SSX TYPE SHIELD] user_data is corrupted (type={type(user_data).__name__}). "
                   f"NUCLEAR state clear initiated.")
      
      # NUCLEAR: Clear ALL conversation state
      if context:
         context.user_data.clear()
         context.chat_data.clear()
         context.bot_data.clear()
      
      # Force conversation end - do NOT proceed with corrupted data
      outputTextList.append("⚠️ Session corruption detected and cleared.\nPlease restart with /start")
      
      return {
          "outputTextList": outputTextList,
          "outputChat_data": {"state": "END"},  # Force end
          "outputUser_data": {},
          "end_conversation": True  # Signal to end conversation
      }
   
   # Now we can safely access user_data as a dict
   admin_id = os.environ.get("TG_ADMIN_ID", "").strip()
   user_id = user_data.get('user_id', '')
   is_admin = admin_id and str(user_id) == admin_id
   
   # ===================================================================
   # DATABASE SURGERY - SELF-HEALING GUARD (SYNCHRONOUS)
   # Check if chat_id data in persistent userdata is corrupted (string instead of dict)
   # This fixes accounts stuck in string format from previous bugs
   # FIX: Synchronous surgery with immediate state reset and re-login requirement
   # ===================================================================
   chat_id = user_data.get('chat_id')
   if chat_id:
       try:
           # Get the raw persistent userdata (by reading file directly)
           import json as _json
           import os as _os
           _data_dir = _os.path.join(_os.path.dirname(__file__), 'data')
           _userdata_path = _os.path.join(_data_dir, 'userdata.json')
           if _os.path.exists(_userdata_path):
               with open(_userdata_path, 'r') as f:
                   persisted_userdata = _json.load(f)
               if chat_id in persisted_userdata and not isinstance(persisted_userdata[chat_id], dict):
                   logger.warning(f"[SSX DATABASE SURGERY] Corrupted string data found for {chat_id}. "
                                  f"Fixing synchronously and forcing re-login.")
                   
                   # CRITICAL: Save synchronously BEFORE continuing
                   persisted_userdata[chat_id] = {}
                   with open(_userdata_path, 'w') as f:
                       _json.dump(persisted_userdata, f)
                   
                   # NUCLEAR: Also clear runtime state
                   if context:
                       context.user_data.clear()
                       context.chat_data.clear()
                   
                   # Force user to re-login
                   outputTextList.append("⚠️ Corrupted profile detected and repaired.\nPlease login again with /start")
                   return {
                       "outputTextList": outputTextList,
                       "outputChat_data": {"state": "END"},  # Force end
                       "outputUser_data": {},
                       "end_conversation": True  # Signal to end conversation
                   }
       except Exception as e:
           logger.error(f"[SSX DATABASE SURGERY] Failed: {e}")
           # If surgery fails, still prevent dispatcher access
           if context:
               context.user_data.clear()
               context.chat_data.clear()
           outputTextList.append("⚠️ Database error - please contact admin")
           return {
               "outputTextList": outputTextList,
               "outputChat_data": {"state": "END"},
               "outputUser_data": {},
               "end_conversation": True
           }
   
   # Normal verification flow continues with clean state
   if is_admin or user_input == stored_passcode:
      statusdict = userdatastore.userfiledetect()
      if statusdict['isfile'] == False:
         logger.error("Missed userdata, created new one at verify.")
      elif statusdict['iscorrect'] == False:
         logger.error("Userdata is corrupted, backuped and created new one at verify.")
      else:
         logger.info("Userdata checked at verify.")
      # ===================================================================
      # SSX PROFILE CREATION - Initialize user profile after verification
      # Creates user profile entry in user_data for spiderDict sync
      # Persists to disk immediately for Ghost Drive sync
      # ===================================================================
      actusername = user_data.get('actualusername')
      chat_id = user_data.get('chat_id')
      
      if actusername and chat_id:
          # ===================================================================
          # SSX PROFILE INITIALIZATION - Ensure nested dict exists
          # Initialize the profile dict if not already present
          # ===================================================================
          if actusername not in user_data:
              user_data[actusername] = {}
          
          # ===================================================================
          # SSX NESTED STORAGE - Store ALL fields inside the profile
          # The Spider sees root-level keys as "drawers" to crawl
          # All user data belongs INSIDE the profile, not at root
          # ===================================================================
          user_data[actusername]['actualusername'] = actusername
          user_data[actusername]['userkey'] = user_data[actusername].get('userkey', '')
          user_data[actusername]['state'] = 'ssx_active'
          user_data[actusername]['init'] = 'ssx_active'
          user_data[actusername]['timestamp'] = time.time()
          
          logger.info(f"[SSX VERIFY] Profile created for {actusername} (key={actusername}, chat_id={chat_id})")
      
      # PERSIST PROFILE TO DISK - Save user_data to userdata.json
      # This ensures Ghost Drive has the profile to sync
      # Key by chat_id so spiderfunction can find it in getspiderinfo()
      if chat_id and actusername:
          try:
              # datastore merges with existing data using dict.update()
              userdatastore.datastore(user_data)
              logger.info(f"[SSX VERIFY] Profile persisted to disk for {actusername}")
          except Exception as e:
              logger.error(f"[SSX VERIFY] Failed to persist profile: {e}")
      
      # ===================================================================
      # SSX IDENTITY LOCK - Use local actusername variable for logging
      # After profile creation, user_data['actualusername'] may have been
      # moved inside the nested dict. Use local variable to ensure
      # "unknown" never appears in logs.
      # ===================================================================
      logger.info("Identity of %s verified", actusername if actusername else 'unknown')
      chat_data.update({"fromadvcreate": False, "fromedit": False, "fromguide": False})
      currentuserdata = userdatastore.dataretrive(actusername=actusername if actusername else 'unknown')
      outputTextGeneralInfo = replytext.GeneralInfo.format(len(currentuserdata), generalcfg.maxiumprofile)
      outputTextList.append(outputTextGeneralInfo)
      if len(currentuserdata) >= generalcfg.maxiumprofile:
         logger.info("User %s has %d profile(s), excess or equal to the maxium profiles limitation.", actusername if actusername else 'unknown', len(currentuserdata))
         outputTextProfileExcessVerify = replytext.ProfileExcessVerify.format(generalcfg.maxiumprofile) 
         outputTextList.append(outputTextProfileExcessVerify)
         chat_data.update({'profileover': True, 'state': 'advance'})
      else:
        outputTextToUserCookies = replytext.ToUserCookies
        outputTextList.append(outputTextToUserCookies)
        chat_data.update({'state': 'usercookies'})
   else:
      outputTextVerifyFail = replytext.VerifyFail
      outputTextList.append(outputTextVerifyFail)
      logger.error("Identity of %s could not be verified", user_data.get('actualusername', 'unknown')) 
      chat_data.update({'state': 'verify'}) 
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
   return outputDict  

def usercookies(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'ADVANCE':
      outputTextCookiesToADV = replytext.CookiesToADV
      outputTextList.append(outputTextCookiesToADV)
      chat_data.update({'state': 'advance'})
   else: 
      try:
         cookies = literal_eval(inputStr)
      except (SyntaxError, TypeError, ValueError):
         logger.error("The INCORRECT cookies of user %s is %s.", user_data.get('actualusername', 'unknown'), inputStr)
         outputTextCookiesError = replytext.CookiesError
         outputTextList.append(outputTextCookiesError)
         chat_data.update({'state': 'usercookies'})
      else:
         logger.info("The cookies of user %s is %s.", user_data.get('actualusername', 'unknown'), str(cookies))
         outputTextToUserKey = replytext.ToUserKey
         outputTextList.append(outputTextToUserKey)
         user_data.update({'usercookies': cookies})
         chat_data.update({'state': 'userkey'})
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict

def userkey(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'RETURN':
      logger.info("User %s has returned to usercookies.", user_data.get('actualusername', 'unknown'))
      outputTextReturnToCookies = replytext.ReturnToCookies 
      outputTextList.append(outputTextReturnToCookies)
      chat_data.update({'state': 'usercookies'})
   else:
      chat_data.update({'state': 'userranges'})
      if inputStr == 'EMPTY':
         user_data.update({"userkey": ""})
         logger.info("The search key of user %s is empty.", user_data.get('actualusername', 'unknown'))
      else:
         user_data.update({"userkey": inputStr,})
         logger.info("The search key of user %s is %s.", user_data.get('actualusername', 'unknown'), inputStr)
      outputTextToUserRange = replytext.ToUserRange
      outputTextList.append(outputTextToUserRange)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict

def userranges(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'RETURN':
      logger.info("User %s has returned to usercookies.", user_data.get('actualusername', 'unknown'))
      outputTextReturnToKey = replytext.ReturnToKey
      outputTextList.append(outputTextReturnToKey)
      chat_data.update({'state': 'userkey'})
   else:
      try:
         ranges = int(inputStr)
      except ValueError:
         logger.info("The INCORRECT input of user %s is %s.", user_data.get('actualusername', 'unknown'), inputStr)
         outputTextRangeError = replytext.RangeError
         outputTextList.append(outputTextRangeError)
         chat_data.update({'state': 'userranges'})
      else:
        if ranges > generalcfg.userPageLimit:
           ranges = generalcfg.userPageLimit
        user_data.update({"userranges": ranges})
        outputTextToUserCate = replytext.ToUserCate
        outputTextList.append(outputTextToUserCate)
        chat_data.update({'state': 'usercate'})
        logger.info("The search range of user %s is %s.", user_data.get('actualusername', 'unknown'), str(ranges))
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict

def usercate(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'RETURN':
      logger.info("User %s has returned to userranges.", user_data.get('actualusername', 'unknown'))
      outputTextReturnToRange = replytext.ReturnToRange
      outputTextList.append(outputTextReturnToRange)
      chat_data.update({'state': 'userranges'})
   else:
      catechecklist = ['doujinshi', 'manga', 'artistcg', 'gamecg', 'western', 'non-h', 'imageset', 'asianporn', 'misc', 'cosplay']
      catelist = inputStr.split(' ')
      if set(catelist).issubset(catechecklist) == True:
         user_data.update({'usercate': catelist})
         chat_data.update({'state': 'userresult'})
         logger.info("The correct search categories of user %s are %s.", user_data.get('actualusername', 'unknown'), inputStr)
         outputTextList.append(replytext.ToUserResult.format((int(generalcfg.interval)/3600)))
      else:
         chat_data.update({'state': 'usercate'})
         logger.info("The INCORRECT search categories of user %s are %s, return to usercate", user_data.get('actualusername', 'unknown'), inputStr)
         outputTextCateError = replytext.CateError
         outputTextList.append(outputTextCateError)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict  

def userresult(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'RETURN':
      logger.info("User %s has returned to usercate.", user_data.get('actualusername', 'unknown'))
      outputTextReturnToCate = replytext.ReturnToCate
      outputTextList.append(outputTextReturnToCate)
      chat_data.update({'state': 'usercate'})
   elif inputStr == 1 or inputStr == "1":
      user_data.update({"resultToChat": True, "userpubchenn": False})
      logger.info("User %s would receive his/her result in the chat.", user_data.get('actualusername', 'unknown'))
      chat_data.update({'state': 'username'})
      outputTextList.append(replytext.ToVirtualUsername)
   elif inputStr == 2 or inputStr == "2":
      user_data.update({"resultToChat": False, "userpubchenn": True})
      logger.info("User %s would receive his/her result in the public channel.", user_data.get('actualusername', 'unknown'))
      chat_data.update({'state': 'username'})
      outputTextList.append(replytext.ToVirtualUsername)
   elif inputStr == 3 or inputStr == "3":
      user_data.update({"resultToChat": True, "userpubchenn": True})
      logger.info("User %s would receive his/her result in both the chat and public channel.", user_data.get('actualusername', 'unknown'))   
      chat_data.update({'state': 'username'})
      outputTextList.append(replytext.ToVirtualUsername)
   else:
      logger.info("User %s's wrong input is %s",
                   user_data.get('actualusername', 'unknown'),
                   inputStr)
      outputTextList.append(replytext.ChoiceError)
      chat_data.update({'state': 'userresult'})
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict     

def username(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'RETURN':
      logger.info("User %s has returned to userpubchenn.", user_data.get('actualusername', 'unknown'))
      outputTextList.append(replytext.ReturnToUserResult)
      chat_data.update({'state': 'userresult'})
   else:
      chat_data.update({'state': 'storeinfo', "fromadvcreate": False, "fromedit": False, "fromguide": True})
      user_data.update({"virtualusername": inputStr})
      logger.info("Virual username of %s is %s.", user_data.get('actualusername', 'unknown'), inputStr)
      outputTextToStoreInfo = replytext.ToStoreInfo 
      outputTextList.append(outputTextToStoreInfo)
      del user_data['chat_id']
      outputTextUserInfo = str(user_data)
      outputTextList.append(outputTextUserInfo)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict     

def storeinfo(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == "YES":
      statusdict = userdatastore.userfiledetect()
      if statusdict['isfile'] == False:
         logger.error("Could not find userdate, create new one at storeinfo")
      elif statusdict['iscorrect'] == False:
         logger.error("Userdata is corrupted, backup the broken file and create new one at storeinfo")
      else:
         logger.info("Userdata checked at storeinfo")
      userdata = {user_data["virtualusername"]: user_data}
      chat_data.update({"virtualusername": user_data["virtualusername"]})
      del  userdata[user_data["virtualusername"]]["virtualusername"]
      if chat_data["fromedit"] == True:
         userdatastore.datadelete(chat_data["oldvirusername"])
      IOreportdict = userdatastore.datastore(userdict=userdata)
      if IOreportdict['issaved'] == IOreportdict['nosamename'] == True:
         logger.info("The information of user %s is saved.", user_data.get('actualusername', 'unknown'))
         outputTextStored = replytext.Stored 
         outputTextList.append(outputTextStored)
         
         chat_data.update({'state': 'END'})
      else:
         logger.error("The information of user %s is NOT saved due to IO issue or name issue.", user_data.get('actualusername', 'unknown'))
         chat_data.update({'state': 'username'})
         outputTextStoreError = replytext.StoreError
         outputTextList.append(outputTextStoreError)
         if chat_data["fromguide"] == True:
            chat_data.update({'state': 'username'})
            logger.error("User %s has returned to username.", user_data.get('actualusername', 'unknown'))
         else:
           chat_data.update({'state': 'advcreate'})
           logger.error("User %s has returned to advcreate.", user_data.get('actualusername', 'unknown'))
   else: 
      if chat_data["fromguide"] == True:
         logger.info("The information of user %s is not saved due to user cancel, return to virtual username.", user_data.get('actualusername', 'unknown'))
         outputTextReturnToVirtualUsername = replytext.ReturnToVirtualUsername
         outputTextList.append(outputTextReturnToVirtualUsername)
         chat_data.update({'state': "username"})
      else: 
         chat_data.update({'state': 'advcreate'})
         logger.info("The information of user %s is not saved due to user cancel, return to advcreate.", user_data.get('actualusername', 'unknown'))
         outputTextReturnToAdvCreate = replytext.ReturnToAdvCreate
         outputTextList.append(outputTextReturnToAdvCreate)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict

def advance(inputStr, user_data, chat_data, logger):
   outputTextList = []
   if inputStr == 'INFO':
      chat_data.update({'state': 'advguide'})
      logger.info("User %s entered advance mode", user_data.get('actualusername', 'unknown'))
      statusdict = userdatastore.userfiledetect()
      if statusdict['isfile'] == False:
         logger.error("Missied userdata, created new one at advance.")
      elif statusdict['iscorrect'] == False:
         logger.error("Userdata is corrupted, backuped and created new one at advance.")
      else:
         logger.info("Userdata checked at advance.")
      currentuserdata = userdatastore.dataretrive(actusername=user_data.get('actualusername', 'unknown'))
      outputTextProfileAmount = replytext.ProfileAmount.format(len(currentuserdata))
      outputTextList.append(outputTextProfileAmount)
      for cd in currentuserdata:
         if 'actualusername' in currentuserdata[cd]:
            del currentuserdata[cd]['actualusername']
         if 'chat_id' in currentuserdata[cd]:
            del currentuserdata[cd]['chat_id']
         outputTextProfileInfo = str(cd) + '\n' + str(currentuserdata[cd])
         outputTextList.append(outputTextProfileInfo)
      outputTextFuncSelect = replytext.FuncSelect
      outputTextList.append(outputTextFuncSelect)
   else:
      chat_data.update({'state': 'advance'})
      outputTextAdvError = replytext.AdvError
      outputTextList.append(outputTextAdvError)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict    

def advguide(inputStr, user_data, chat_data, logger):
   outputTextList = []
   currentuserdata = userdatastore.dataretrive(actusername=user_data.get('actualusername', 'unknown'))
   logger.info("User %s entered advguide", user_data.get('actualusername', 'unknown'))
   if inputStr == 'ADVCREATE':
      statusdict = userdatastore.userfiledetect()
      if statusdict['isfile'] == False:
         logger.error("Missied userdata, created new one.")
      elif statusdict['iscorrect'] == False:
         logger.error("Userdata is broken, backup and created new one.")
      else:
         logger.info("Userdata checked at advguide.")
      if 'profileover' in chat_data:
         logger.info("User %s already has %d profile(s) so could not create new one.", 
                     user_data.get('actualusername', 'unknown'), 
                     len(currentuserdata)
                     )
         outputTextProfileExcess = replytext.ProfileExcess
         outputTextList.append(outputTextProfileExcess)
         chat_data.update({'state:': 'advguide'})
      else:
         chat_data.update({'state': 'advcreate'})
         outputTextToAdvCreate = replytext.ToAdvCreate
         outputTextList.append(outputTextToAdvCreate)
   elif inputStr == 'ADVEDIT':
      if len(currentuserdata) == 0:
         chat_data.update({'state': 'advguide'})
         logger.info("User %s does not has any profile", user_data.get('actualusername', 'unknown'))
         outputTextNoProfile = replytext.NoProfile
         outputTextList.append(outputTextNoProfile)
      else:
         chat_data.update({'state': 'advedit'})
         outputTextSelectProfileNameToEdit = replytext.SelectProfileNameToEdit
         outputTextList.append(outputTextSelectProfileNameToEdit)
   elif inputStr == 'DELETE':
      if len(currentuserdata) == 0:
         chat_data.update({'state': 'advguide'})
         outputTextNoProfileToDelete = replytext.NoProfileToDelete
         outputTextList.append(outputTextNoProfileToDelete)
      else:
         chat_data.update({'state': 'delete'})
         outputTextSelectProfileNameToDelete = replytext.SelectProfileNameToDelete
         outputTextList.append(outputTextSelectProfileNameToDelete)
   else:
      chat_data.update({'state': 'advguide'})
      outputTextErrorInput = replytext.ErrorInput
      outputTextList.append(outputTextErrorInput)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict

def advcreate(inputStr, user_data, chat_data, logger):
   outputTextList = []
   logger.info("User %s has entered advcreate.", user_data.get('actualusername', 'unknown'))
   try:
      tempuserdata = literal_eval(inputStr)
   except (SyntaxError, TypeError, ValueError):
      chat_data.update({'state': 'advcreate'})
      outputTextErrorSyntax = replytext. ErrorSyntax
      outputTextList.append(outputTextErrorSyntax)
      logger.info("The INCORRECT profile of user %s is %s, return to advcreate", user_data.get('actualusername', 'unknown'), inputStr)
   else:
      userdatachecklist = ['usercate', 'userranges', 'userkey', 'resultToChat', 'userpubchenn', 'virtualusername', 'usercookies']
      catechecklist = ['doujinshi', 'manga', 'artistcg', 'gamecg', 'western', 'non-h', 'imageset', 'asianporn', 'misc', 'cosplay']
      if set(userdatachecklist) != set(list(tempuserdata.keys())):       
         logger.info("The INCORRECT userdata key of user %s is %s, return to advcreate", user_data.get('actualusername', 'unknown'), str(tempuserdata.keys()))
         chat_data.update({'state': 'advcreate'})
         outputTextUserDataCheckFail = replytext.UserDataCheckFail
         outputTextList.append(outputTextUserDataCheckFail)

      elif type(tempuserdata['usercate']) != list:
         logger.info("The INCORRECT usercate type of user %s is %s, return to advcreate", user_data.get('actualusername', 'unknown'), str(type(tempuserdata['usercate']))) 
         chat_data.update({'state': 'advcreate'})
         outputTextUserCateSyntaxError = replytext.UserCateSyntaxError
         outputTextList.append(outputTextUserCateSyntaxError)

      elif set(tempuserdata['usercate']).issubset(catechecklist) == False:
         logger.info("The INCORRECT usercate categories of user %s is %s, return to advcreate", user_data.get('actualusername', 'unknown'), str(tempuserdata['usercate'])) 
         chat_data.update({'state': 'advcreate'})
         outputTextUserCateCheckFail = replytext.UserCateCheckFail
         outputTextList.append(outputTextUserCateCheckFail)

      elif type(tempuserdata["userranges"]) != int:
         logger.info("The INCORRECT userranges type of user %s is %s, return to advcreate", user_data.get('actualusername', 'unknown'), str(type(tempuserdata["userranges"]))) 
         chat_data.update({'state': 'advcreate'})
         outputTextUserRangesValueError = replytext.UserRangesValueError
         outputTextList.append(outputTextUserRangesValueError)
      elif tempuserdata["resultToChat"] == False and tempuserdata["userpubchenn"] == False:
         logger.info("User %s does not choice any method to receive the result", user_data.get('actualusername', 'unknown'))
         outputTextList.append(replytext.UserReceiveResultError)
         chat_data.update({'state': 'advcreate'})
      else:
         if tempuserdata["userranges"] > generalcfg.userPageLimit:
            logger.error("The INCORRECT userranges value of user %s is %d, Limit to %d", user_data.get('actualusername', 'unknown'), tempuserdata["userranges"], generalcfg.userPageLimit) 
            tempuserdata["userranges"] = generalcfg.userPageLimit
            outputTextRangeExcess = replytext.RangeExcess
            outputTextList.append(outputTextRangeExcess)
         if chat_data["fromedit"] == True:
            logger.info("The correct usedata of user %s from advedit is %s.", user_data.get('actualusername', 'unknown'), str(tempuserdata))
            chat_data.update({'state': 'storeinfo'})
         else:        
            logger.info("The correct usedata of user %s is %s.", user_data.get('actualusername', 'unknown'), str(tempuserdata))
            chat_data.update({"fromadvcreate": True, "fromedit": False, "fromguide": False, 'state': 'storeinfo'})
         user_data.update(tempuserdata)
         outputTextUserDataCheckComplete = replytext.UserDataCheckComplete
         outputTextList.append(outputTextUserDataCheckComplete)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict

def advedit(inputStr, user_data, chat_data, logger):
   outputTextList = []
   tempuserdata = userdatastore.dataretrive(user_data.get('actualusername', 'unknown'))
   if tempuserdata.get(inputStr, 'None') == 'None':
      chat_data.update({'state': 'advedit'})
      outputTextErrorVirtualUserName = replytext.ErrorVirtualUserName
      outputTextList.append(outputTextErrorVirtualUserName)
   else:
      chat_data.update({"fromadvcreate": False, "fromedit": True, 
                        "fromguide": False, "oldvirusername": inputStr,
                        'state': 'advcreate'})
      logger.info("User %s is going to edit %s", user_data.get('actualusername', 'unknown'), inputStr)
      tempuserdata = tempuserdata[inputStr]
      tempuserdata.update({"virtualusername": inputStr})
      if 'actualusername' in tempuserdata:
         del tempuserdata["actualusername"]
      if 'chat_id' in tempuserdata:
         del tempuserdata["chat_id"]
      outputTextRetriveProfileSuccess = replytext.RetriveProfileSuccess
      outputTextList.append(outputTextRetriveProfileSuccess)
      outputTextProfileContent = str(tempuserdata)
      outputTextList.append(outputTextProfileContent)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict      

def delete(inputStr, user_data, chat_data, logger):
   outputTextList = []
   logger.info("User %s has entered delete.", user_data.get('actualusername', 'unknown'))
   IOreportdict = userdatastore.datadelete(inputStr)
   if IOreportdict["hasdata"] == False:
      chat_data.update({'state': 'delete'})
      logger.error("User %s's virtual userdata %s not found.", 
                   user_data.get('actualusername', 'unknown'), 
                   inputStr
                  )
      outputTextVirUsernameNotFound = replytext.VirUsernameNotFound
      outputTextList.append(outputTextVirUsernameNotFound)
   else:
      chat_data.update({'state': 'advance'})
      logger.info("User %s's virtual userdata %s has been deleted.", 
                   user_data.get('actualusername', 'unknown'), 
                   inputStr
                 )
      outputTextDeleteSuccess = replytext.DeleteSuccess.format(inputStr)
      outputTextList.append(outputTextDeleteSuccess)
   outputDict = {"outputTextList": outputTextList,
                 "outputChat_data": chat_data, 
                 "outputUser_data": user_data
                }
#    print (user_data)
#    print (chat_data)
   return outputDict  

# Global bot reference for sending error messages
_spider_bot = None

def set_spider_bot(bot):
    """Set the bot instance for error message delivery."""
    global _spider_bot
    _spider_bot = bot

def spiderfunction(logger, spiderDict=None, chat_id=None):
   '''This function would either exploit the provided user information or the information on
      disk (if user information is not provided) to search the e-h/exh and return several 
      Manga objects. Every Manga object contains all of a gallery's information. This function
      would create a dict like {username1: MangaObjectList1, username2: MangaObjectList2} to 
      handle multiple users' result'''
   
   # =======================================================================
   # METADATA ISOLATION - Filter out _metadata before processing
   # System metadata is now stored in _metadata sub-dict to prevent namespace pollution
   # The dispatcher should only process user profile keys (not _metadata)
   # =======================================================================
   if spiderDict is not None:
       # Check if spiderDict itself is corrupted (not a dict)
       if not isinstance(spiderDict, dict):
           logger.error(f"[SSX DISPATCHER SHIELD] Rejected non-dict spiderDict: type={type(spiderDict)}")
           return {}
       
       # METADATA ISOLATION: Remove _metadata from spiderDict before processing
       # _metadata contains system fields like timestamp, version, init flags
       # These are NOT user profiles and should not be processed by the dispatcher
       if '_metadata' in spiderDict:
           logger.info("[SSX METADATA ISOLATION] Filtering out _metadata from spiderDict")
           del spiderDict['_metadata']
       
       # LEGACY GARBAGE FILTER: Remove any legacy top-level garbage keys
       # These may exist from older backups before metadata isolation was implemented
       legacy_invalid_keys = {'ssx_active', 'timestamp', 'version', 'ghost_drive_init', 'init', 'last_sync_timestamp', 'sync_version'}
       garbage_removed = False
       for key in list(spiderDict.keys()):
           if key in legacy_invalid_keys:
               logger.info(f"[SSX LEGACY CLEANUP] Removing legacy garbage key: {key}")
               del spiderDict[key]
               garbage_removed = True
       
       if garbage_removed:
           logger.info(f"[SSX LEGACY CLEANUP] SpiderDict keys after cleanup: {list(spiderDict.keys())[:10]}")
       
       # If spiderDict is now empty, return empty
       if not spiderDict or len(spiderDict) == 0:
           logger.info("[SSX DISPATCHER] spiderDict empty after metadata filtering, returning empty")
           return {}
   
   # =======================================================================
   # MASSIVE TRY/EXCEPT ERROR TRAP
   # Catches ALL exceptions and sends ERROR MESSAGE directly to user
   # This prevents silent failures and tells you WHY it crashed
   # =======================================================================
   try:
       if spiderDict == None:
          spiderDict = userdatastore.getspiderinfo()
       else: 
          # print (spiderDict)
          for sD in spiderDict:
             # ===================================================================
             # COMPREHENSIVE TYPE-GUARD WITH ORPHAN FILTERING
             # Fix AttributeError and KeyError: spiderDict[sD] may be any non-dict 
             # type (string, float, boolean) from fresh/empty or corrupted userdata.
             # Convert any non-dict to a safe settings dict with chat_id mapping.
             # Filter out garbage orphan values to save API calls.
             # ===================================================================
             if not isinstance(spiderDict[sD], dict):
                val_str = str(spiderDict[sD])
                
                # Filter out obvious non-keyword orphans to save API calls
                # Skip boolean strings, init markers, and numeric values (timestamps)
                if val_str in ['True', 'False', 'ssx_active'] or val_str.replace('.','',1).replace('-','',1).isdigit():
                    logger.info("Skipping garbage orphan value '%s'", val_str)
                    continue
                
                # Map the loop key (sd) back to chat_id for result delivery
                spiderDict[sD] = {
                    'keywords': [val_str],
                    'chat_id': sD,  # Map loop key to chat_id for result delivery
                    'resultToChat': True
                }
                logger.info("Converted orphan value '%s' to settings dict with chat_id", val_str)
             
             # Now spiderDict[sD] is guaranteed to be a dictionary
             spiderDict[sD].update({'userpubchenn': False, 'resultToChat': True})
       logger.info("Spider is initialing")

       toTelegramDict = {} 
       sendUserResultDict = {} # Determin whether this user has chat_id and channel id to receive the result.
                               # Otherwise the spider would not search this user's information.
       sleep = generator.Sleep(sleepstr=generalcfg.searchInterval)

       for sd in spiderDict:
          # ===================================================================
          # DISPATCHER SHIELDING (azuriteshift assist)
          # Verify spiderDict[sd] is actually a dict before calling .get()
          # This prevents AttributeError when hitting string orphans
          # ===================================================================
          if not isinstance(spiderDict.get(sd), dict):
             logger.warning("Dispatcher skipping garbage entry '%s' at key '%s'", 
                           str(spiderDict.get(sd)), str(sd))
             continue
          
          tempChat_idList = []
          # Use .get() for safety to prevent KeyError if dict is malformed
          if spiderDict[sd].get('resultToChat') == True and spiderDict[sd].get('chat_id'):
             tempChat_idList.append(spiderDict[sd].get('chat_id'))
          # Use .get() for safety - prevent KeyError if dict is malformed
          if spiderDict[sd].get("userpubchenn") == True and generalcfg.pubChannelID:
             tempChat_idList.append(generalcfg.pubChannelID)
          sendUserResultDict.update({sd: tempChat_idList})
       for sd in sendUserResultDict: 
          # ===================================================================
          # DISPATCHER SHIELDING (azuriteshift assist)
          # Verify spiderDict[sd] is actually a dict before dispatch
          # This makes the dispatcher "blind" to leftover strings/trash
          # ===================================================================
          if not isinstance(spiderDict[sd], dict):
             logger.warning("Dispatcher skipping garbage entry '%s' at key '%s'", 
                           str(spiderDict.get(sd)), str(sd))
             continue
          
          if sendUserResultDict[sd]:
             logger.info("Search user %s's information", str(sd))
             generator.Sleep.Havearest(sleep)
             searchopt = searchoptgen.searchgenerate(generateDict=spiderDict[sd])
             cookies = spiderDict[sd].get("usercookies")
             userResultStorePath = "./searchresult/{0}/{1}/".format(spiderDict[sd].get("actualusername", sd), sd)
             imageObjList = exhspider.Spidercontrolasfunc(searchopt=searchopt, 
                                                          cookies=cookies, 
                                                          path=userResultStorePath,
                                                          logger=logger,
                                                          datastore=userdatastore.datastore,
                                                          spiderDict=spiderDict,
                                                          sd=sd
                                                         ) 
             logger.info("Search of user %s has completed.", str(sd))

             if imageObjList:
                toTelegramDict.update({sd: imageObjList})
          else:
             pass
       return toTelegramDict
   
   # =======================================================================
   # ERROR HANDLER - Send crash details directly to user
   # =======================================================================
   except Exception as e:
       import traceback
       error_msg = f"🔴 SPIDER CRASH REPORT 🔴\n\n"
       error_msg += f"Error: {type(e).__name__}: {str(e)}\n\n"
       error_msg += f"Traceback:\n{traceback.format_exc()[:1000]}"
       
       logger.error(f"[SSX SPIDER CRASH] {error_msg}")
       
       # Try to send error message directly to user
       target_chat_id = chat_id
       if target_chat_id is None and spiderDict:
           # Try to get chat_id from spiderDict
           for sd in spiderDict:
               if spiderDict[sd].get('chat_id'):
                   target_chat_id = spiderDict[sd].get('chat_id')
                   break
       
       if target_chat_id and _spider_bot:
           try:
               import asyncio
               loop = asyncio.new_event_loop()
               asyncio.set_event_loop(loop)
               loop.run_until_complete(_spider_bot.send_message(
                   chat_id=target_chat_id,
                   text=error_msg
               ))
               loop.close()
           except Exception as notify_err:
               logger.error(f"[SSX SPIDER] Failed to send crash notification: {notify_err}")
       
       return {}  # Return empty dict so bot continues running


def messageanalyze(inputStr=None, user_data=None, chat_data=None, logger=None, context=None):
   '''This function controls the interaction between user and bot. The basic working
      method of this function is that the upper layers provide the user input, status
      and user information. Then, this function would exploit a suitable sub function
      to treat these message and return the result. After that, it would return these 
      results to the upper layers.'''
   messageFuncDict = {'verify': verify,
                      'usercookies': usercookies,
                      'userkey': userkey,
                      'userranges': userranges,
                      'usercate': usercate,
                      'userresult': userresult,
                      'username': username,
                      'storeinfo': storeinfo,
                      'advance': advance,
                      'advguide': advguide,
                      'advcreate': advcreate,
                      'advedit': advedit,
                      'delete': delete
                     }
   
   # Pass context only to verify function (which needs it for nuclear state clearing)
   if chat_data['state'] == 'verify':
       outputDict = messageFuncDict['verify'](inputStr=inputStr, 
                                               user_data=user_data, 
                                               chat_data=chat_data,
                                               logger=logger,
                                               context=context
                                              )
   else:
       outputDict = messageFuncDict[chat_data['state']](inputStr=inputStr, 
                                                        user_data=user_data, 
                                                        chat_data=chat_data,
                                                        logger=logger
                                                       )
   return outputDict
