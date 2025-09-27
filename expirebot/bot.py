## Expirebot
## A plugin for Matrix that allows users to set message expiration times for rooms.

import asyncio
import datetime
import re
from typing import Type

from maubot import Plugin
from maubot.handlers import command, event
from mautrix.types import (MessageEvent, EventType, MessageType, StateEvent, Membership)

from .db import upgrade_table

def normalize_text(text: str) -> str:
    """
    Lowercase and remove all non-alphanumeric characters for matching.
    Example: "K.ill!" -> "kill"
    """
    return re.sub(r'[^a-z0-9]', '', text.lower())

def parse_duration(duration_str: str) -> int:
    """
    Parse a string such as "24h", "3d", "15m", or composite durations like "1d2h" into seconds.
    Raises ValueError if the format is invalid.
    """
    # Match optional days, hours, minutes, seconds (all parts optional but at least one must be present)
    pattern = re.compile(
        r"^((?P<days>\d+?)d)?\s*((?P<hours>\d+?)h)?\s*((?P<minutes>\d+?)m)?\s*((?P<seconds>\d+?)s)?$"
    )
    match = pattern.fullmatch(duration_str.strip())
    if not match or not any(match.group(g) for g in ("days", "hours", "minutes", "seconds")):
        raise ValueError(f"Invalid duration format: {duration_str}")

    parts = {name: int(val) for name, val in match.groupdict(default="0").items()}
    td = datetime.timedelta(
        days=parts["days"],
        hours=parts["hours"],
        minutes=parts["minutes"],
        seconds=parts["seconds"],
    )
    return int(td.total_seconds() * 1000) # return expiration time in ms

class ExpiringMessages(Plugin):

    _expirer_task: asyncio.Task = None
    _redaction_semaphore: asyncio.Semaphore = None
    _last_redaction_time: float = 0
    _min_redaction_interval: float = 0.1  # Minimum 100ms between redactions

    async def get_bad_words(self) -> list[str]:
        rows = await self.database.fetch("SELECT word FROM bad_words")
        return [r["word"] for r in rows]

    async def add_bad_word(self, word: str) -> bool:
        try:
            await self.database.execute(
                "INSERT INTO bad_words (word) VALUES ($1) ON CONFLICT DO NOTHING",
                word.lower(),
            )
            return True
        except Exception as e:
            self.log.error(f"Failed to insert bad word {word}: {e}")
            return False

    async def delete_bad_word(self, word: str) -> bool:
        try:
            await self.database.execute(
                "DELETE FROM bad_words WHERE word=$1",
                word.lower(),
            )
            return True
        except Exception as e:
            self.log.error(f"Failed to delete bad word {word}: {e}")
            return False

    async def can_use_command(self, evt: MessageEvent) -> tuple[bool, str]:
        """
        Check if both the user and bot have permission to redact messages in this room.
        Returns a tuple of (has_permission, error_message).
        """
        try:
            # Get the room's power levels
            levels = await self.client.get_state_event(
                evt.room_id, EventType.ROOM_POWER_LEVELS
            )
            
            # Get the user's power level
            user_level = levels.get_user_level(evt.sender)
            
            # Get the bot's power level
            bot_level = levels.get_user_level(self.client.mxid)
            
            # Get the power level required for redaction
            redact_level = getattr(levels, 'redact', 50)  # Default to 50 if not set
            
            # Debug logging
            self.log.debug(f"Permission check for room {evt.room_id}:")
            self.log.debug(f"  User {evt.sender} has level {user_level}")
            self.log.debug(f"  Bot {self.client.mxid} has level {bot_level}")
            self.log.debug(f"  Required redaction level is {redact_level}")
            
            # Check if user has permission
            if user_level < redact_level:
                return False, f"You need a power level of {redact_level} or higher to run this command."
            
            # Check if bot has permission
            if bot_level < redact_level:
                return False, f"I need a power level of {redact_level} or higher to function."
            
            return True, ""
        except Exception as e:
            self.log.error(f"Error checking command permissions: {e}")
            return False, "Failed to check permissions. Please try again later."

    async def _redact_with_backoff(self, room_id: str, event_id: str, max_retries: int = 5) -> bool:
        """
        Attempt to redact an event with exponential backoff.
        Returns True if successful, False if failed after all retries.
        """
        base_delay = 1  # Start with 1 second delay
        max_delay = 32  # Maximum delay of 32 seconds
        
        for attempt in range(max_retries):
            try:
                # Ensure minimum time between redactions
                now = asyncio.get_event_loop().time()
                time_since_last = now - self._last_redaction_time
                if time_since_last < self._min_redaction_interval:
                    await asyncio.sleep(self._min_redaction_interval - time_since_last)
                
                async with self._redaction_semaphore:
                    await self.client.redact(room_id, event_id, reason="Message expired")
                    self._last_redaction_time = asyncio.get_event_loop().time()
                    return True
            except Exception as e:
                if "Too Many Requests" in str(e):
                    delay = min(base_delay * (2 ** attempt), max_delay)  # Exponential backoff, capped at max_delay
                    self.log.warning(f"Rate limited while redacting {event_id}. Retrying in {delay} seconds...")
                    await asyncio.sleep(delay)
                else:
                    self.log.error(f"Failed to redact event {event_id}: {e}")
                    return False
        
        self.log.error(f"Failed to redact event {event_id} after {max_retries} attempts")
        return False

    async def _process_expirations(self):
        now_ms = int(datetime.datetime.now(datetime.UTC).timestamp() * 1000)
        try:
            # Use a more compatible query that works with both SQLite and PostgreSQL
            events_query = """
                SELECT e.event_id, e.room_id, r.expiry_msec as expiry_msec
                FROM events e
                INNER JOIN room_expiry_times r ON r.room_id = e.room_id
            """
            events_to_expire = await self.database.fetch(events_query)
            
            # Process events in smaller batches to avoid overwhelming the server
            batch_size = 10
            for i in range(0, len(events_to_expire), batch_size):
                batch = events_to_expire[i:i + batch_size]
                
                for event in batch:
                    room_id = event['room_id']
                    expiration_ms = event['expiry_msec']
                    cutoff = now_ms - expiration_ms
                    
                    try:
                        event_content = await self.client.get_event(room_id, event['event_id'])
                        if event_content == None:
                            self.log.info(f"Event {event['event_id']} is empty, scrapping.")
                            # Delete the event from our database after successful redaction
                            await self.database.execute(
                                "DELETE FROM events WHERE event_id = $1",
                                event['event_id']
                            )
                            self.log.info(f"Event {event['event_id']} scrapped.")
                        elif event_content.timestamp < cutoff:
                            if await self._redact_with_backoff(room_id, event['event_id']):
                                # Delete the event from our database after successful redaction
                                await self.database.execute(
                                    "DELETE FROM events WHERE event_id = $1",
                                    event['event_id']
                                )
                                self.log.info(f"Redacted event {event['event_id']} in room {room_id}")
                                self.log.info(f"Removed event {event['event_id']} from redaction tracking database")
                            else:
                                self.log.error(f"Failed to redact event {event['event_id']} after all retries")
                    except Exception as e:
                        self.log.error(f"Failed to process event {event['event_id']}: {e}")
                
                # Add a small delay between batches
                if i + batch_size < len(events_to_expire):
                    await asyncio.sleep(0.5)
        except Exception as e:
            self.log.error(f"Database error in _process_expirations: {e}")

    async def _expirer_loop(self):
        """
        Periodic task that scans recent room history for messages that have become expired and redacts them.
        This loop runs every 60 seconds.
        """
        while True:
            try:
                await self._process_expirations()
            except Exception as e:
                self.log.error("Error in expiration loop: %s", e)
            await asyncio.sleep(60)

    async def start(self) -> None:
        await super().start()
        # Database migrations will ensure RoomExpiration table exists.
        self._expirer_task = asyncio.create_task(self._expirer_loop())
        self._redaction_semaphore = asyncio.Semaphore(1)  # Only one redaction at a time

        # ✅ Load bad words from DB
        self.bad_words = set(await self.get_bad_words())
        self.log.info(f"Loaded {len(self.bad_words)} bad words from database.")


    async def stop(self) -> None:
        if self._expirer_task:
            self._expirer_task.cancel()
        await super().stop()


    @command.new("badword", help="Manage global bad words (admin only)")
    async def badword_cmd(self, evt: MessageEvent) -> None:
        allowed, msg = await self.can_use_command(evt)
        await evt.respond("Usage:\n• !badword add <word>\n• !badword del <word>\n• !badword list")

    @badword_cmd.subcommand("add", help="Add a bad word (admin only)")
    @command.argument("word", "Word to add")
    async def badword_add(self, evt: MessageEvent, word: str) -> None:
        allowed, msg = await self.can_use_command(evt)
        if not allowed:
            await evt.respond(msg)
            return
        if await self.add_bad_word(word):
            self.bad_words.add(word.lower())
            await evt.respond(f"✅ Added bad word: `{word}`")
        else:
            await evt.respond("Failed to add bad word.")

    @badword_cmd.subcommand("del", help="Delete a bad word (admin only)")
    @command.argument("word", "Word to delete")
    async def badword_del(self, evt: MessageEvent, word: str) -> None:
        allowed, msg = await self.can_use_command(evt)
        if not allowed:
            await evt.respond(msg)
            return
        if await self.delete_bad_word(word):
            self.bad_words.discard(word.lower())
            await evt.respond(f"🗑️ Deleted bad word: `{word}`")
        else:
            await evt.respond("Failed to delete bad word.")

    @badword_cmd.subcommand("list", help="List all bad words")
    async def badword_list(self, evt: MessageEvent) -> None:
        allowed, msg = await self.can_use_command(evt)
        words = sorted(self.bad_words)
        if words:
            await evt.respond("🚫 **Bad words:**\n" + ", ".join(words))
        else:
            await evt.respond("No bad words set.")

    @command.new("expire", help="Configure message expiration for rooms")
    async def cmd_expire(self, evt: MessageEvent) -> None:
        """Base command for message expiration settings"""
        await evt.respond("Available subcommands:\n"
                         "  !expire set <time> - Set expiration time (e.g. !expire set 24h)\n"
                         "  !expire unset - Disable message expiration\n"
                         "  !expire show - Show current expiration settings")

    @cmd_expire.subcommand("set", help="Set message expiration for this room")
    @command.argument("time_arg", "expiry time, e.g. 24h or 1d2h30m")
    async def cmd_expire_set(self, evt: MessageEvent, time_arg: str) -> None:
        room_id = evt.room_id

        ## room participants must have appropriate PL to set message expiration
        has_permission, error_msg = await self.can_use_command(evt)
        if not has_permission:
            await evt.respond(error_msg)
            return None

        # Handle setting expiration time
        try:
            mseconds = parse_duration(time_arg)
        except ValueError as e:
            await evt.respond(f"Error parsing duration: {e}")
            return
        
        try:
            # Use REPLACE INTO for SQLite compatibility
            query = """
                INSERT INTO room_expiry_times(room_id, expiry_msec)
                VALUES ($1, $2)
                ON CONFLICT(room_id) DO UPDATE SET expiry_msec=$2
            """
            await self.database.execute(query, room_id, mseconds)
            await evt.respond(f"Message expiration for this room set to {time_arg}")
        except Exception as e:
            self.log.error(f"Database error in cmd_expire_set: {e}")
            await evt.respond("Failed to update room expiration settings. Please try again later.")

    @cmd_expire.subcommand("unset", help="Disable message expiration for this room")
    async def cmd_expire_unset(self, evt: MessageEvent) -> None:
        room_id = evt.room_id

        has_permission, error_msg = await self.can_use_command(evt)
        if not has_permission:
            await evt.respond(error_msg)
            return None

        try:
            # Delete the room rule - events will be automatically deleted due to ON DELETE CASCADE
            await self.database.execute(
                "DELETE FROM room_expiry_times WHERE room_id = $1",
                room_id
            )
            await evt.respond("Message expiration for this room has been disabled. All tracked messages will be preserved.")
        except Exception as e:
            self.log.error(f"Database error in cmd_expire_unset: {e}")
            await evt.respond("Failed to disable room expiration. Please try again later.")

    @cmd_expire.subcommand("show", help="Show current message expiration settings for this room")
    async def cmd_expire_show(self, evt: MessageEvent) -> None:
        room_id = evt.room_id
        try:
            query = """
                SELECT expiry_msec FROM room_expiry_times
                WHERE room_id = $1
            """
            result = await self.database.fetchrow(query, room_id)
            
            # Build the response message
            response_parts = []
            
            if result:
                # Convert milliseconds back to human readable format
                msec = result['expiry_msec']
                days = msec // (24 * 60 * 60 * 1000)
                msec %= (24 * 60 * 60 * 1000)
                hours = msec // (60 * 60 * 1000)
                msec %= (60 * 60 * 1000)
                minutes = msec // (60 * 1000)
                msec %= (60 * 1000)
                seconds = msec // 1000
                
                parts = []
                if days > 0:
                    parts.append(f"{days}d")
                if hours > 0:
                    parts.append(f"{hours}h")
                if minutes > 0:
                    parts.append(f"{minutes}m")
                if seconds > 0:
                    parts.append(f"{seconds}s")
                
                duration = "".join(parts)
                response_parts.append(f"📅 **Message Expiration:** {duration}")
            else:
                response_parts.append("📅 **Message Expiration:** Not configured")
            
            # Check bot permissions
            try:
                levels = await self.client.get_state_event(
                    room_id, EventType.ROOM_POWER_LEVELS
                )
                bot_level = levels.get_user_level(self.client.mxid)
                redact_level = getattr(levels, 'redact', 50)  # Default to 50 if not set
                
                if bot_level >= redact_level:
                    response_parts.append("✅ **Bot Permissions:** Sufficient permissions to redact messages")
                else:
                    response_parts.append(f"⚠️ **Bot Permissions:** Insufficient permissions (current: {bot_level}, required: {redact_level}+)")
                
                # Add user permission info
                user_level = levels.get_user_level(evt.sender)
                if user_level >= redact_level:
                    response_parts.append("✅ **Your Permissions:** You can configure message expiration")
                else:
                    response_parts.append(f"⚠️ **Your Permissions:** You need power level {redact_level}+ to configure expiration (current: {user_level})")
                    
            except Exception as perm_error:
                self.log.error(f"Failed to check permissions in room {room_id}: {perm_error}")
                response_parts.append("⚠️ **Bot Permissions:** Unable to verify permissions")
            
            # Send the complete response
            await evt.respond("\n\n".join(response_parts))
            
        except Exception as e:
            self.log.error(f"Database error in cmd_expire_show: {e}")
            await evt.respond("Failed to fetch room expiration settings. Please try again later.")

    @event.on(EventType.ROOM_MESSAGE)
    async def track_expiring_message(self, evt: MessageEvent) -> None:
        if evt.content.msgtype not in {
            MessageType.TEXT,
            MessageType.NOTICE,
            MessageType.EMOTE,
            MessageType.FILE,
            MessageType.IMAGE,
            MessageType.VIDEO,
            MessageType.LOCATION,
        }:
            return

        # --- Word filter ---
        body = getattr(evt.content, "body", "") or ""
        normalized = normalize_text(body)
        for bad in self.bad_words:
            if normalize_text(bad) in normalized:
                try:
                    # Use your _redact_with_backoff method to safely redact
                    success = await self._redact_with_backoff(evt.room_id, evt.event_id)
                    if success:
                        self.log.info(f"Deleted bad word in {evt.room_id}: {bad}")
                    else:
                        self.log.error(f"Failed to delete bad word in {evt.room_id}: {bad}")
                except Exception as e:
                    self.log.error(f"Failed to delete filtered message: {e}")
                return

        # --- Expiration tracking ---
        try:
            rule = await self.database.fetchrow(
                "SELECT expiry_msec FROM room_expiry_times WHERE room_id=$1",
                evt.room_id,
            )
            if rule:
                await self.database.execute(
                    "INSERT INTO events(event_id, room_id) VALUES ($1, $2)",
                    evt.event_id,
                    evt.room_id,
                )
        except Exception as e:
            self.log.error(f"track_expiring_message DB error: {e}")

    @event.on(EventType.STICKER)
    async def track_expiring_sticker(self, evt) -> None:
        try:
            room_rules = await self.database.fetch("SELECT room_id, expiry_msec FROM room_expiry_times")
            
            # Check if this room has an expiration rule
            room_rule = next((rule for rule in room_rules if rule['room_id'] == evt.room_id), None)
            if room_rule:
                query = """
                    INSERT INTO events(event_id, room_id)
                    VALUES ($1, $2)
                """
                await self.database.execute(query, evt.event_id, evt.room_id)
        except Exception as e:
            self.log.error(f"Database error in track_expiring_sticker: {e}")
            # Don't respond to the user since this is an event handler

    @event.on(EventType.ROOM_MEMBER)
    async def handle_membership_change(self, evt: StateEvent) -> None:
        """
        Handle room membership changes. When the bot leaves a room, clean up expiration settings.
        When the bot joins a room, set default expiration and make announcements.
        """
        # Only process events where the bot's membership is changing
        if evt.state_key != self.client.mxid:
            return
        
        room_id = evt.room_id
        
        # Check if the bot is leaving the room
        if evt.content.membership == Membership.LEAVE:
            # Check if this room has expiration settings that need to be cleaned up
            existing_settings = await self.database.fetchrow(
                "SELECT expiry_msec FROM room_expiry_times WHERE room_id = $1",
                room_id
            )
            
            if existing_settings:
                self.log.info(f"Bot leaving room {room_id}, cleaning up expiration settings")
                
                try:
                    # Delete the room's expiration rule and all tracked events
                    # The events will be automatically deleted due to ON DELETE CASCADE
                    await self.database.execute(
                        "DELETE FROM room_expiry_times WHERE room_id = $1",
                        room_id
                    )
                    self.log.info(f"Cleaned up expiration settings for room {room_id}")
                except Exception as e:
                    self.log.error(f"Failed to clean up expiration settings for room {room_id}: {e}")
            else:
                self.log.info(f"Bot leaving room {room_id} that has no expiration settings to clean up")
        
        # Check if the bot is joining the room
        elif evt.content.membership == Membership.JOIN:
            # Check if this room already has expiration settings configured
            existing_settings = await self.database.fetchrow(
                "SELECT expiry_msec FROM room_expiry_times WHERE room_id = $1",
                room_id
            )
            
            # Only set defaults and send greeting if this is a new room (no existing settings)
            if not existing_settings:
                self.log.info(f"Bot joining new room {room_id}, setting default expiration")
                
                try:
                    # Set default 7-day expiration (7 days in milliseconds)
                    default_expiry_ms = 7 * 24 * 60 * 60 * 1000
                    
                    # Use REPLACE INTO for SQLite compatibility
                    query = """
                        INSERT INTO room_expiry_times(room_id, expiry_msec)
                        VALUES ($1, $2)
                        ON CONFLICT(room_id) DO UPDATE SET expiry_msec=$2
                    """
                    await self.database.execute(query, room_id, default_expiry_ms)
                    
                    greeting = '\n'.join([
                        "🤖 Hi, I'm a message expiration bot!",
                        "",
                        "📅 Default Configuration: Messages will automatically expire after 7 days",
                        "",
                        "💡 Commands:",
                        "• !expire set <time> - Change expiration time (e.g., !expire set 24h)",
                        "• !expire unset - Disable message expiration", 
                        "• !expire show - Show current settings",
                        ""
                    ])

                    # Check if the bot has necessary permissions
                    try:
                        levels = await self.client.get_state_event(
                            room_id, EventType.ROOM_POWER_LEVELS
                        )
                        bot_level = levels.get_user_level(self.client.mxid)
                        redact_level = getattr(levels, 'redact', 50)  # Default to 50 if not set
                        
                        if bot_level >= redact_level:
                            greeting_status = "✅ Status: Bot has necessary permissions to redact messages"
                        else:
                            greeting_status = "⚠️ Warning: Bot needs power level " + str(redact_level) + " or higher to redact messages. " \
                                            "Current level: " + str(bot_level) + ". Please grant appropriate permissions."
                        
                        greeting += greeting_status

                    except Exception as perm_error:
                        greeting_status = "⚠️ Warning: Unable to verify bot permissions. Please ensure the bot has power level 50+ to redact messages."
                        greeting += greeting_status
                        self.log.error(f"Failed to check permissions in room {room_id}: {perm_error}")
                    finally:
                        await self.client.send_notice(room_id, greeting)
                        self.log.info(f"Set default 7-day expiration for room {room_id}")
                    
                except Exception as e:
                    self.log.error(f"Failed to set default expiration for room {room_id}: {e}")
                    greeting_status = "⚠️ Warning: Something unexpected happened, and I was unable to configure default message expiration. " \
                                    "Please use !expire set 7d to manually configure it."
                    greeting += greeting_status
                    await self.client.send_notice(room_id, greeting)
            else:
                self.log.info(f"Bot joining room {room_id} that already has expiration settings configured")

    @classmethod
    def get_db_upgrade_table(cls) -> None:
        return upgrade_table

