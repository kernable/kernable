# Backups and restores

## What is backed up

kernable Cloud backs up each **copy**: what your readers follow. The new parts every few
seconds (every five at most) and the whole copy daily, kept seven days, in object storage in the
EU, apart from the relays. The backups are sealed like the copy: kernable cannot read them.

## What is not

**Your writer.** Its store is in your infrastructure, and so are its backups: a copy is not a
way to get a writer back, and a reader is never promoted to a writer. Back up your writer with
the kernel's own backup follower:
[backups](/how-to/backup), and keep a
[standby](/how-to/standby) beside it.

## When kernable restores

If a relay loses a copy, kernable restores it from its backups: the newest full and every part
after it, checked to fit together. Your pusher then pushes on from where the restored copy ends;
if your writer no longer keeps those records, it sends an image and the copy starts anew from it.
Your readers follow as before. You do nothing.

Restores are rehearsed: a drill restores copies from their backups byte for byte and lets a
reader follow the result.

## If your writer was restored

If you restored your writer from an older backup, its log no longer continues the copy. The
pusher notices and pushes nothing: the copy keeps its history, so a writer started by mistake
cannot erase it. The pusher's log says so as an error, with the copy's position. If the restored
writer is meant to replace the copy, start the pusher once with `--start-anew <position>` (in a
node's `project.toml`, `start-anew = <position>` under `[push]`): it empties the copy and fills it
again from the writer's first record. Your readers start anew from it, as from a writer.
