from mautrix.util.async_db import UpgradeTable, Connection

upgrade_table = UpgradeTable()

@upgrade_table.register(description="Table initialization")
async def upgrade_v1(conn: Connection) -> None:
    # Create room_expiry_times table
    await conn.execute(
        """CREATE TABLE room_expiry_times (
            room_id TEXT PRIMARY KEY,
            expiry_msec INTEGER NOT NULL
        )"""
    )
    
    # Create events table with foreign key to room_expiry_times
    await conn.execute(
        """CREATE TABLE events (
            event_id TEXT PRIMARY KEY,
            room_id TEXT NOT NULL,
            FOREIGN KEY(room_id) REFERENCES room_expiry_times(room_id) ON DELETE CASCADE
        )"""
    )
    
    # Create bad_words table
    await conn.execute(
        """CREATE TABLE bad_words (
            word TEXT PRIMARY KEY
        )"""
    )
    
    # Create index on events.room_id for faster lookups
    await conn.execute(
        """CREATE INDEX idx_events_room_id ON events(room_id)"""
    )

@upgrade_table.register(description="Add index for faster room lookups")
async def upgrade_v2(conn: Connection) -> None:
    # Some databases may already have the index
    try:
        await conn.execute("CREATE INDEX idx_events_room_id ON events(room_id)")
    except Exception:
        pass
