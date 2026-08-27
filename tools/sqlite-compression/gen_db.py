#!/usr/bin/env python3
"""Generate zero-cache-replica-shaped SQLite databases for compression analysis.

Mirrors what zero-cache actually writes to disk:
  * no declared PRIMARY KEY (see pg-to-lite.ts: "PRIMARY KEYS are not written to
    the replica. Instead, we rely on UNIQUE indexes")
  * a `_0_version` TEXT column on every table
  * lite type strings ("varchar|NOT_NULL") as the declared column types
  * every upstream index recreated

Two schemas:
  zbugs   -- real zbugs schema + real gigabugs text templates. nanoid text PKs.
  chinook -- classic Chinook schema, scaled up. integer PKs, short text.
"""

import argparse
import json
import os
import random
import sqlite3
import string
import sys
import time

NANOID_ALPHABET = string.ascii_letters + string.digits + "_-"
LEXI_ALPHABET = string.digits + string.ascii_lowercase


def nanoid(rng, n=21):
    return "".join(rng.choices(NANOID_ALPHABET, k=n))


class Versions:
    """`_0_version` generator. Cardinality matters: every row of every table
    carries one, so whether it is a constant or a random string moves the
    whole-file ratio.

      constant -- every row shares one version. What a replica looks like
                  immediately after initial-sync, i.e. a fresh base image.
      mixed    -- 70% initial-sync version, 30% drawn from a pool of recent
                  transaction watermarks. A replica that has been live a while.
      random   -- a distinct version per row. Not realistic; the pessimistic
                  bound.
    """

    def __init__(self, rng, mode="mixed", pool=200_000):
        self.rng = rng
        self.mode = mode
        self.base = self._draw()
        self.pool = [self._draw() for _ in range(pool)] if mode == "mixed" else None

    def _draw(self):
        return "7" + "".join(self.rng.choices(LEXI_ALPHABET, k=6))

    def __call__(self):
        if self.mode == "constant":
            return self.base
        if self.mode == "random":
            return self._draw()
        return self.base if self.rng.random() < 0.7 else self.rng.choice(self.pool)


def connect(path):
    if os.path.exists(path):
        os.remove(path)
    for suffix in ("-wal", "-shm", "-journal"):
        if os.path.exists(path + suffix):
            os.remove(path + suffix)
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    db.execute("PRAGMA cache_size=-1000000")  # 1GB page cache
    db.execute("PRAGMA locking_mode=EXCLUSIVE")
    return db


# --------------------------------------------------------------------------
# zbugs
# --------------------------------------------------------------------------

ZBUGS_DDL = [
    """CREATE TABLE "user" (
        "id" "varchar|NOT_NULL", "login" "varchar|NOT_NULL", "name" "varchar",
        "avatar" "varchar", "role" "varchar|NOT_NULL|TEXT_ENUM",
        "githubID" "int4|NOT_NULL", "email" "varchar",
        "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "project" (
        "id" "varchar|NOT_NULL", "name" "varchar|NOT_NULL",
        "lowerCaseName" "varchar|NOT_NULL", "issueCountEstimate" "int4",
        "supportsSearch" "bool|NOT_NULL", "markURL" "varchar", "logoURL" "varchar",
        "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "issue" (
        "id" "varchar|NOT_NULL", "shortID" "int4", "title" "varchar|NOT_NULL",
        "open" "bool|NOT_NULL", "modified" "float8", "created" "float8",
        "projectID" "varchar|NOT_NULL", "creatorID" "varchar|NOT_NULL",
        "assigneeID" "varchar", "description" "varchar",
        "visibility" "varchar|NOT_NULL|TEXT_ENUM",
        "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "comment" (
        "id" "varchar|NOT_NULL", "issueID" "varchar", "created" "float8",
        "body" "text|NOT_NULL", "creatorID" "varchar",
        "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "label" (
        "id" "varchar|NOT_NULL", "name" "varchar|NOT_NULL",
        "projectID" "varchar|NOT_NULL", "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "issueLabel" (
        "labelID" "varchar|NOT_NULL", "issueID" "varchar|NOT_NULL",
        "projectID" "varchar|NOT_NULL", "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "emoji" (
        "id" "varchar|NOT_NULL", "value" "varchar|NOT_NULL",
        "annotation" "varchar", "subjectID" "varchar|NOT_NULL",
        "creatorID" "varchar", "created" "float8", "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "viewState" (
        "userID" "varchar|NOT_NULL", "issueID" "varchar|NOT_NULL",
        "viewed" "float8", "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "userPref" (
        "key" "varchar|NOT_NULL", "value" "varchar|NOT_NULL",
        "userID" "varchar|NOT_NULL", "_0_version" TEXT NOT NULL)""",
    """CREATE TABLE "issueNotifications" (
        "userID" "varchar|NOT_NULL", "issueID" "varchar|NOT_NULL",
        "subscribed" "bool", "created" "float8", "_0_version" TEXT NOT NULL)""",
]

# Upstream PK -> UNIQUE INDEX (what zero-cache creates), plus every real index
# from apps/zbugs/db/migrations/*.sql.
ZBUGS_INDEXES = [
    'CREATE UNIQUE INDEX "user_pkey" ON "user" ("id")',
    'CREATE UNIQUE INDEX "user_login_idx" ON "user" ("login")',
    'CREATE UNIQUE INDEX "user_githubid_idx" ON "user" ("githubID")',
    'CREATE UNIQUE INDEX "project_pkey" ON "project" ("id")',
    'CREATE UNIQUE INDEX "project_name_idx" ON "project" ("name")',
    'CREATE UNIQUE INDEX "project_lower_case_name_idx" ON "project" ("lowerCaseName")',
    'CREATE UNIQUE INDEX "issue_pkey" ON "issue" ("id")',
    'CREATE UNIQUE INDEX "issue_project_idx" ON "issue" ("id","projectID")',
    'CREATE INDEX "issue_created_idx" ON "issue" ("created")',
    'CREATE INDEX "issue_modified_idx" ON "issue" ("modified")',
    'CREATE INDEX "issue_open_modified_idx" ON "issue" ("open","modified")',
    'CREATE INDEX "issue_shortID_idx" ON "issue" ("shortID")',
    'CREATE INDEX "issue_projectID_open_assigneeID_modified_idx" ON "issue" ("projectID","open","assigneeID","modified","id")',
    'CREATE INDEX "issue_projectID_open_creatorID_modified_idx" ON "issue" ("projectID","open","creatorID","modified","id")',
    'CREATE INDEX "issue_projectID_open_modified_idx" ON "issue" ("projectID","open","modified","id")',
    'CREATE INDEX "issue_creatorID_idx" ON "issue" ("creatorID","id")',
    'CREATE INDEX "issue_assigneeID_idx" ON "issue" ("assigneeID","id")',
    'CREATE INDEX "issue_projectID_assigneeID_modified_idx" ON "issue" ("projectID","assigneeID","modified","id")',
    'CREATE INDEX "issue_projectID_creatorID_modified_idx" ON "issue" ("projectID","creatorID","modified","id")',
    'CREATE INDEX "issue_projectID_modified_idx" ON "issue" ("projectID","modified","id")',
    'CREATE UNIQUE INDEX "comment_pkey" ON "comment" ("id")',
    'CREATE INDEX "comment_issueid_idx" ON "comment" ("issueID")',
    'CREATE UNIQUE INDEX "label_pkey" ON "label" ("id")',
    'CREATE UNIQUE INDEX "label_project_idx" ON "label" ("id","projectID")',
    'CREATE INDEX "label_name_idx" ON "label" ("name")',
    'CREATE UNIQUE INDEX "label_name_project_idx" ON "label" ("projectID","name")',
    'CREATE UNIQUE INDEX "issueLabel_pkey" ON "issueLabel" ("labelID","issueID")',
    'CREATE INDEX "issuelabel_issueid_idx" ON "issueLabel" ("issueID")',
    'CREATE UNIQUE INDEX "emoji_pkey" ON "emoji" ("id")',
    'CREATE INDEX "emoji_created_idx" ON "emoji" ("created")',
    'CREATE INDEX "emoji_subject_id_idx" ON "emoji" ("subjectID")',
    'CREATE UNIQUE INDEX "viewState_pkey" ON "viewState" ("userID","issueID")',
    'CREATE UNIQUE INDEX "userPref_pkey" ON "userPref" ("userID","key")',
    'CREATE UNIQUE INDEX "issueNotifications_pkey" ON "issueNotifications" ("userID","issueID")',
]


def load_templates(root):
    """Load the real gigabugs seed templates from apps/zbugs/db/seed-data/templates."""
    titles, descs, comments, labels, components, projects = [], [], [], [], [], []
    tdir = os.path.join(root, "apps/zbugs/db/seed-data/templates")
    for fn in sorted(os.listdir(tdir)):
        if not fn.endswith(".json") or fn == "summary.json":
            continue
        d = json.load(open(os.path.join(tdir, fn)))
        titles += d["titleTemplates"]
        descs += d["descriptionTemplates"]
        comments += d["commentTemplates"]
        labels += d["labels"]
        for p in d["projects"]:
            projects.append(p["name"])
            components += p["components"]
    return titles, descs, comments, sorted(set(labels)), components, sorted(set(projects))


FILLERS = {
    "action": ["saving a draft", "switching tabs", "resizing the window",
               "submitting the form", "scrolling rapidly", "opening the modal",
               "importing a CSV", "toggling dark mode", "logging back in"],
    "environment": ["Chrome 128 on macOS", "Safari 17 iOS", "Firefox 129 Linux",
                    "Edge 127 Windows 11", "Chrome Android 14", "staging", "production"],
    "error": ["TypeError: undefined is not a function", "ECONNRESET",
              "NullPointerException", "OOMKilled", "HTTP 502 Bad Gateway",
              "SIGSEGV in worker thread", "deadlock detected"],
    "steps": ["1) open the app 2) click the button 3) observe the crash",
              "reproduce by loading a 10k row grid and sorting twice",
              "start the service, wait 30s, then send a burst of requests"],
    "expected": ["the row is saved", "no error is shown", "the page renders",
                 "latency stays under 100ms", "the job completes"],
    "actual": ["the tab freezes", "a blank screen appears", "the request times out",
               "memory grows without bound", "the worker restarts"],
    "version": ["1.4.2", "2.0.0-rc3", "3.11.9", "0.9.14", "5.2.1"],
    "dependency": ["react-dom", "grpc-js", "libssl", "tokio", "sqlite3", "protobuf"],
    "suggestion": ["Pinning the transitive dep", "Adding a debounce",
                   "Moving the work off the main thread", "Bumping the pool size"],
    "component": None,  # filled from the project component lists
}


HEX = "0123456789abcdef"
PKGS = ["core", "api", "worker", "ui", "auth", "sync", "store", "net", "codec", "sched"]
MODS = ["handler", "client", "server", "pool", "cache", "queue", "parser", "writer",
        "reader", "router", "session", "stream", "buffer", "index", "shard"]


def unique_tail(rng):
    """A stack trace / log excerpt with per-row unique tokens. Real bug reports
    carry this kind of content (addresses, hashes, ids, paths, line numbers) and
    it is what actually resists compression -- unlike a templated corpus, where
    the same few hundred descriptions repeat verbatim across millions of rows."""
    frames = []
    for _ in range(rng.randint(3, 7)):
        frames.append("  at %s/%s.%s:%d:%d (0x%s)" % (
            rng.choice(PKGS), rng.choice(MODS),
            rng.choice(["run", "flush", "commit", "poll", "send", "recv", "tick"]),
            rng.randint(10, 4000), rng.randint(1, 120),
            "".join(rng.choices(HEX, k=12))))
    return ("\n\nTrace id %s, build %s\n%s\nrequest=%s span=%s" % (
        "".join(rng.choices(HEX, k=32)),
        "".join(rng.choices(HEX, k=40)),
        "\n".join(frames),
        "".join(rng.choices(HEX, k=16)),
        "".join(rng.choices(HEX, k=16))))


def fill(rng, template, components):
    out = template
    while "{{" in out:
        i = out.index("{{")
        j = out.index("}}", i)
        key = out[i + 2:j]
        if key == "component":
            val = rng.choice(components)
        else:
            opts = FILLERS.get(key)
            val = rng.choice(opts) if opts else rng.choice(components)
        out = out[:i] + val + out[j + 2:]
    return out


# Measured heap bytes per unit and heap-fraction-of-final-file, used only to
# size the first batch; the loop then self-corrects from live measurements.
ZBUGS_HEAP_PER_ISSUE = 2176
ZBUGS_HEAP_FRACTION = 0.585
CHINOOK_HEAP_PER_TRACK = 174
CHINOOK_HEAP_FRACTION = 0.573


def next_batch(heap, goal, units, floor, cap):
    """Size the next batch to land on `goal` without overshooting."""
    per = heap / units
    return max(floor, min(int((goal - heap) / per), cap))


def issue_text(rng, templates, components, text_entropy):
    body = fill(rng, rng.choice(templates), components)
    if text_entropy == "high":
        body += unique_tail(rng)
    return body


def gen_zbugs(db, target_bytes, root, rng, versions, text_entropy):
    titles, descs, comments, labels, components, projnames = load_templates(root)
    for stmt in ZBUGS_DDL:
        db.execute(stmt)

    n_projects = len(projnames)
    db.executemany(
        'INSERT INTO "project" VALUES (?,?,?,?,?,?,?,?)',
        [(nanoid(rng), p, p.lower(), rng.randint(1000, 900000), 1, None, None,
          versions()) for p in projnames])
    project_ids = [r[0] for r in db.execute('SELECT "id" FROM "project"')]

    label_rows = []
    for pid in project_ids:
        for name in labels:
            label_rows.append((nanoid(rng), name, pid, versions()))
    db.executemany('INSERT INTO "label" VALUES (?,?,?,?)', label_rows)
    labels_by_project = {}
    for lid, name, pid, _ in label_rows:
        labels_by_project.setdefault(pid, []).append(lid)

    # Row-count ratios modelled on the gigabugs seed shape.
    # per issue: 3.5 comments, 2.5 issueLabels, 1.5 emoji, 4 viewStates,
    #            0.5 issueNotifications; 1 user per 50 issues.
    page = db.execute("PRAGMA page_size").fetchone()[0]
    goal = target_bytes * ZBUGS_HEAP_FRACTION
    per_issue = ZBUGS_HEAP_PER_ISSUE * (3 if text_entropy == "high" else 1)
    batch = max(1000, min(100_000, int(goal / per_issue / 4)))
    issues_done = 0
    user_seq = 0
    users = []
    short_id = 3000
    emoji_values = ["+1", "-1", "eyes", "rocket", "tada", "heart", "confused", "laugh"]
    emoji_annotations = ["thumbs up", "thumbs down", "eyes", "rocket",
                         "party popper", "red heart", "confused face", "grinning"]

    while True:
        # users for this batch
        new_users = []
        for _ in range(max(1, batch // 50)):
            # login and githubID both carry UNIQUE indexes upstream. Draw them
            # from a counter rather than at random: at a few tens of thousands
            # of users, random draws collide by the birthday bound.
            user_seq += 1
            uid = nanoid(rng)
            login = "".join(rng.choices(string.ascii_lowercase, k=rng.randint(5, 12))) + str(user_seq)
            new_users.append((uid, login,
                              login.capitalize() + " " + rng.choice(["Smith", "Chen", "Patel", "Garcia", "Kim", "Novak"]),
                              "https://avatars.githubusercontent.com/u/%d?v=4" % rng.randint(1, 99999999),
                              rng.choice(["user", "crew"]), 1_000_000 + user_seq,
                              login + "@example.com", versions()))
        db.executemany('INSERT INTO "user" VALUES (?,?,?,?,?,?,?,?)', new_users)
        users += [u[0] for u in new_users]

        db.executemany('INSERT INTO "userPref" VALUES (?,?,?,?)',
                       [(k, rng.choice(["true", "false"]), u[0], versions())
                        for u in new_users for k in ("emailNotifications", "theme")])

        issue_rows, comment_rows, il_rows, emoji_rows, vs_rows, notif_rows = [], [], [], [], [], []
        for _ in range(batch):
            iid = nanoid(rng)
            pid = rng.choice(project_ids)
            creator = rng.choice(users)
            assignee = rng.choice(users) if rng.random() < 0.6 else None
            created = 1600000000000 + rng.random() * 200000000000
            modified = created + rng.random() * 10000000000
            short_id += 1
            issue_rows.append((
                iid, short_id, fill(rng, rng.choice(titles), components),
                1 if rng.random() < 0.35 else 0, modified, created, pid, creator,
                assignee, issue_text(rng, descs, components, text_entropy),
                "public" if rng.random() < 0.9 else "internal", versions()))

            for _ in range(rng.choice([1, 2, 3, 4, 5, 6])):
                comment_rows.append((nanoid(rng), iid, created + rng.random() * 1e9,
                                     issue_text(rng, comments, components, text_entropy),
                                     rng.choice(users), versions()))
            for lid in rng.sample(labels_by_project[pid], rng.choice([1, 2, 3, 4])):
                il_rows.append((lid, iid, pid, versions()))
            for _ in range(rng.choice([0, 1, 2, 3])):
                k = rng.randrange(len(emoji_values))
                emoji_rows.append((nanoid(rng), emoji_values[k], emoji_annotations[k],
                                   iid, rng.choice(users), created + rng.random() * 1e9,
                                   versions()))
            for u in rng.sample(users, min(len(users), rng.choice([1, 2, 3, 4, 5, 6, 7]))):
                vs_rows.append((u, iid, created + rng.random() * 1e9, versions()))
            if rng.random() < 0.5:
                notif_rows.append((rng.choice(users), iid, 1, created, versions()))

        db.executemany('INSERT INTO "issue" VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', issue_rows)
        db.executemany('INSERT INTO "comment" VALUES (?,?,?,?,?,?)', comment_rows)
        db.executemany('INSERT OR IGNORE INTO "issueLabel" VALUES (?,?,?,?)', il_rows)
        db.executemany('INSERT INTO "emoji" VALUES (?,?,?,?,?,?,?)', emoji_rows)
        db.executemany('INSERT OR IGNORE INTO "viewState" VALUES (?,?,?,?)', vs_rows)
        db.executemany('INSERT OR IGNORE INTO "issueNotifications" VALUES (?,?,?,?,?)', notif_rows)
        db.commit()
        issues_done += batch

        pages = db.execute("PRAGMA page_count").fetchone()[0]
        cur = pages * page
        sys.stderr.write("  zbugs: %d issues, heap %.2f GB\n" % (issues_done, cur / 1e9))
        sys.stderr.flush()
        if cur >= goal:
            break
        batch = next_batch(cur, goal, issues_done, 1000, 400_000)
    return issues_done


# --------------------------------------------------------------------------
# chinook
# --------------------------------------------------------------------------

CHINOOK_DDL = [
    '''CREATE TABLE "Artist" ("ArtistId" "int4|NOT_NULL", "Name" "varchar",
        "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Album" ("AlbumId" "int4|NOT_NULL", "Title" "varchar|NOT_NULL",
        "ArtistId" "int4|NOT_NULL", "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Track" ("TrackId" "int4|NOT_NULL", "Name" "varchar|NOT_NULL",
        "AlbumId" "int4", "MediaTypeId" "int4|NOT_NULL", "GenreId" "int4",
        "Composer" "varchar", "Milliseconds" "int4|NOT_NULL", "Bytes" "int4",
        "UnitPrice" "numeric|NOT_NULL", "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Genre" ("GenreId" "int4|NOT_NULL", "Name" "varchar",
        "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "MediaType" ("MediaTypeId" "int4|NOT_NULL", "Name" "varchar",
        "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Playlist" ("PlaylistId" "int4|NOT_NULL", "Name" "varchar",
        "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "PlaylistTrack" ("PlaylistId" "int4|NOT_NULL",
        "TrackId" "int4|NOT_NULL", "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Customer" ("CustomerId" "int4|NOT_NULL",
        "FirstName" "varchar|NOT_NULL", "LastName" "varchar|NOT_NULL",
        "Company" "varchar", "Address" "varchar", "City" "varchar",
        "State" "varchar", "Country" "varchar", "PostalCode" "varchar",
        "Phone" "varchar", "Fax" "varchar", "Email" "varchar|NOT_NULL",
        "SupportRepId" "int4", "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Employee" ("EmployeeId" "int4|NOT_NULL",
        "LastName" "varchar|NOT_NULL", "FirstName" "varchar|NOT_NULL",
        "Title" "varchar", "ReportsTo" "int4", "BirthDate" "timestamp",
        "HireDate" "timestamp", "Address" "varchar", "City" "varchar",
        "State" "varchar", "Country" "varchar", "PostalCode" "varchar",
        "Phone" "varchar", "Fax" "varchar", "Email" "varchar",
        "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "Invoice" ("InvoiceId" "int4|NOT_NULL",
        "CustomerId" "int4|NOT_NULL", "InvoiceDate" "timestamp|NOT_NULL",
        "BillingAddress" "varchar", "BillingCity" "varchar",
        "BillingState" "varchar", "BillingCountry" "varchar",
        "BillingPostalCode" "varchar", "Total" "numeric|NOT_NULL",
        "_0_version" TEXT NOT NULL)''',
    '''CREATE TABLE "InvoiceLine" ("InvoiceLineId" "int4|NOT_NULL",
        "InvoiceId" "int4|NOT_NULL", "TrackId" "int4|NOT_NULL",
        "UnitPrice" "numeric|NOT_NULL", "Quantity" "int4|NOT_NULL",
        "_0_version" TEXT NOT NULL)''',
]

CHINOOK_INDEXES = [
    'CREATE UNIQUE INDEX "Artist_pkey" ON "Artist" ("ArtistId")',
    'CREATE UNIQUE INDEX "Album_pkey" ON "Album" ("AlbumId")',
    'CREATE INDEX "IFK_AlbumArtistId" ON "Album" ("ArtistId")',
    'CREATE UNIQUE INDEX "Track_pkey" ON "Track" ("TrackId")',
    'CREATE INDEX "IFK_TrackAlbumId" ON "Track" ("AlbumId")',
    'CREATE INDEX "IFK_TrackGenreId" ON "Track" ("GenreId")',
    'CREATE INDEX "IFK_TrackMediaTypeId" ON "Track" ("MediaTypeId")',
    'CREATE UNIQUE INDEX "Genre_pkey" ON "Genre" ("GenreId")',
    'CREATE UNIQUE INDEX "MediaType_pkey" ON "MediaType" ("MediaTypeId")',
    'CREATE UNIQUE INDEX "Playlist_pkey" ON "Playlist" ("PlaylistId")',
    'CREATE UNIQUE INDEX "PlaylistTrack_pkey" ON "PlaylistTrack" ("PlaylistId","TrackId")',
    'CREATE INDEX "IFK_PlaylistTrackTrackId" ON "PlaylistTrack" ("TrackId")',
    'CREATE UNIQUE INDEX "Customer_pkey" ON "Customer" ("CustomerId")',
    'CREATE INDEX "IFK_CustomerSupportRepId" ON "Customer" ("SupportRepId")',
    'CREATE UNIQUE INDEX "Employee_pkey" ON "Employee" ("EmployeeId")',
    'CREATE INDEX "IFK_EmployeeReportsTo" ON "Employee" ("ReportsTo")',
    'CREATE UNIQUE INDEX "Invoice_pkey" ON "Invoice" ("InvoiceId")',
    'CREATE INDEX "IFK_InvoiceCustomerId" ON "Invoice" ("CustomerId")',
    'CREATE UNIQUE INDEX "InvoiceLine_pkey" ON "InvoiceLine" ("InvoiceLineId")',
    'CREATE INDEX "IFK_InvoiceLineInvoiceId" ON "InvoiceLine" ("InvoiceId")',
    'CREATE INDEX "IFK_InvoiceLineTrackId" ON "InvoiceLine" ("TrackId")',
]

GENRES = ["Rock", "Jazz", "Metal", "Alternative & Punk", "Rock And Roll", "Blues",
          "Latin", "Reggae", "Pop", "Soundtrack", "Bossa Nova", "Easy Listening",
          "Heavy Metal", "R&B/Soul", "Electronica/Dance", "World", "Hip Hop/Rap",
          "Science Fiction", "TV Shows", "Sci Fi & Fantasy", "Drama", "Comedy",
          "Alternative", "Classical", "Opera"]
MEDIA_TYPES = ["MPEG audio file", "Protected AAC audio file",
               "Protected MPEG-4 video file", "Purchased AAC audio file",
               "AAC audio file"]
WORDS = ("Love Time Night Heart Blue Sky Fire Rain Dream Road Star Moon Soul Wild "
         "Gold Silver River Stone Shadow Light Dance King Queen City Train Angel "
         "Devil Summer Winter Song Voice Hand Eye Door Wall Sea Sun Storm Ghost "
         "Machine Electric Broken Silent Endless Golden Crystal Velvet Neon").split()
FIRST = "James Mary John Patricia Robert Jennifer Michael Linda William Elizabeth Luis Ana Hugh Frank Helena Astrid Daan Kara Eduardo Alexandre".split()
LAST = "Smith Johnson Williams Brown Jones Garcia Miller Davis Rodriguez Martinez Goncalves Tremblay Peeters Schneider Rojas Nielsen Wichterlova Girard Stevens Murray".split()
CITIES = "Berlin Paris London Oslo Madrid Rome Prague Vienna Lisbon Dublin Boston Chicago Toronto Montreal Mumbai Delhi Tokyo Sydney Warsaw Budapest".split()
COUNTRIES = "Germany France USA Canada Brazil India Portugal Spain Italy Norway Sweden Denmark Poland Austria Ireland Australia Japan Chile Argentina Belgium".split()
STREETS = "Main Oak Pine Maple Cedar Elm Washington Lake Hill Park Church Market Bridge Station Garden".split()


def gen_chinook(db, target_bytes, rng, versions):
    for stmt in CHINOOK_DDL:
        db.execute(stmt)
    db.executemany('INSERT INTO "Genre" VALUES (?,?,?)',
                   [(i + 1, g, versions()) for i, g in enumerate(GENRES)])
    db.executemany('INSERT INTO "MediaType" VALUES (?,?,?)',
                   [(i + 1, m, versions()) for i, m in enumerate(MEDIA_TYPES)])
    db.executemany('INSERT INTO "Playlist" VALUES (?,?,?)',
                   [(i + 1, n, versions()) for i, n in enumerate(
                       ["Music", "Movies", "TV Shows", "Audiobooks", "90's Music",
                        "Classical", "Brazilian Music", "Heavy Metal Classic",
                        "On-The-Go 1", "Grunge", "Music Videos", "Classical 101 - Deep Cuts",
                        "Classical 101 - Next Steps", "Classical 101 - The Basics",
                        "Movies 2", "Audiobooks 2", "TV Shows 2", "Music 2"])])
    db.executemany('INSERT INTO "Employee" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   [(i + 1, LAST[i], FIRST[i], "Sales Support Agent",
                     None if i == 0 else 1, "1970-01-01 00:00:00",
                     "2002-01-01 00:00:00", "%d %s St" % (rng.randint(1, 999), rng.choice(STREETS)),
                     rng.choice(CITIES), "NA", rng.choice(COUNTRIES),
                     "%05d" % rng.randint(1, 99999), "+1 (555) 555-%04d" % rng.randint(0, 9999),
                     None, "%s@chinookcorp.com" % FIRST[i].lower(), versions())
                    for i in range(8)])

    page = db.execute("PRAGMA page_size").fetchone()[0]
    goal = target_bytes * CHINOOK_HEAP_FRACTION
    batch = max(10_000, min(500_000, int(goal / CHINOOK_HEAP_PER_TRACK / 4)))
    track_id = album_id = artist_id = customer_id = invoice_id = line_id = 0

    while True:
        artists = [(artist_id + i + 1,
                    "%s %s" % (rng.choice(WORDS), rng.choice(["Band", "Trio", "Quartet",
                                                              "Orchestra", "Project", "Collective"])),
                    versions()) for i in range(max(1, batch // 50))]
        artist_id += len(artists)
        db.executemany('INSERT INTO "Artist" VALUES (?,?,?)', artists)

        albums = [(album_id + i + 1,
                   "%s %s %s" % (rng.choice(WORDS), rng.choice(WORDS), rng.choice(WORDS)),
                   rng.randint(1, artist_id), versions()) for i in range(max(1, batch // 10))]
        album_id += len(albums)
        db.executemany('INSERT INTO "Album" VALUES (?,?,?,?)', albums)

        tracks = []
        for i in range(batch):
            tid = track_id + i + 1
            tracks.append((tid,
                           "%s %s" % (rng.choice(WORDS), rng.choice(WORDS)),
                           rng.randint(1, album_id), rng.randint(1, len(MEDIA_TYPES)),
                           rng.randint(1, len(GENRES)),
                           "%s %s/%s %s" % (rng.choice(FIRST), rng.choice(LAST),
                                            rng.choice(FIRST), rng.choice(LAST))
                           if rng.random() < 0.7 else None,
                           rng.randint(30000, 600000), rng.randint(1000000, 20000000),
                           0.99, versions()))
        track_id += len(tracks)
        db.executemany('INSERT INTO "Track" VALUES (?,?,?,?,?,?,?,?,?,?)', tracks)

        # (PlaylistId, TrackId) is the PK: sample distinct playlists per track so
        # the rows are unique by construction (the unique index is built later).
        pt = []
        for t in tracks:
            for pl in rng.sample(range(1, 19), rng.choice([1, 1, 2, 2, 3])):
                pt.append((pl, t[0], versions()))
        db.executemany('INSERT INTO "PlaylistTrack" VALUES (?,?,?)', pt)

        customers = []
        for i in range(max(1, batch // 100)):
            cid = customer_id + i + 1
            fn, ln = rng.choice(FIRST), rng.choice(LAST)
            customers.append((cid, fn, ln,
                              "%s %s Inc." % (rng.choice(WORDS), rng.choice(WORDS)) if rng.random() < 0.2 else None,
                              "%d %s St" % (rng.randint(1, 9999), rng.choice(STREETS)),
                              rng.choice(CITIES), "NA", rng.choice(COUNTRIES),
                              "%05d" % rng.randint(1, 99999),
                              "+1 (555) 555-%04d" % rng.randint(0, 9999), None,
                              "%s.%s%d@example.com" % (fn.lower(), ln.lower(), cid),
                              rng.randint(1, 8), versions()))
        customer_id += len(customers)
        db.executemany('INSERT INTO "Customer" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', customers)

        invoices = []
        for i in range(max(1, batch // 5)):
            iid = invoice_id + i + 1
            invoices.append((iid, rng.randint(1, customer_id),
                             "20%02d-%02d-%02d 00:00:00" % (rng.randint(9, 25), rng.randint(1, 12), rng.randint(1, 28)),
                             "%d %s St" % (rng.randint(1, 9999), rng.choice(STREETS)),
                             rng.choice(CITIES), "NA", rng.choice(COUNTRIES),
                             "%05d" % rng.randint(1, 99999),
                             round(rng.randint(1, 25) * 0.99, 2), versions()))
        invoice_id += len(invoices)
        db.executemany('INSERT INTO "Invoice" VALUES (?,?,?,?,?,?,?,?,?,?)', invoices)

        lines = []
        for inv in invoices:
            for _ in range(rng.randint(1, 9)):
                line_id += 1
                lines.append((line_id, inv[0], rng.randint(1, track_id), 0.99, 1, versions()))
        db.executemany('INSERT INTO "InvoiceLine" VALUES (?,?,?,?,?,?)', lines)
        db.commit()

        cur = db.execute("PRAGMA page_count").fetchone()[0] * page
        sys.stderr.write("  chinook: %d tracks, heap %.2f GB\n" % (track_id, cur / 1e9))
        sys.stderr.flush()
        if cur >= goal:
            break
        batch = next_batch(cur, goal, track_id, 10_000, 2_000_000)
    return track_id


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schema", choices=["zbugs", "chinook"], required=True)
    ap.add_argument("--gb", type=float, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--root", default="/home/user/zero-mono")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--page-size", type=int, default=4096)
    ap.add_argument("--version-mode", choices=["constant", "mixed", "random"],
                    default="mixed",
                    help="cardinality of the _0_version column")
    ap.add_argument("--text-entropy", choices=["template", "high"], default="template",
                    help="template = the gigabugs seed corpus verbatim (very "
                         "repetitive); high = adds per-row unique traces/ids")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    versions = Versions(rng, args.version_mode)
    target = args.gb * 1e9
    t0 = time.time()
    db = connect(args.out)
    db.execute("PRAGMA page_size=%d" % args.page_size)

    if args.schema == "zbugs":
        gen_zbugs(db, target, args.root, rng, versions, args.text_entropy)
        indexes = ZBUGS_INDEXES
    else:
        gen_chinook(db, target, rng, versions)
        indexes = CHINOOK_INDEXES

    heap = os.path.getsize(args.out)
    sys.stderr.write("heap done %.2f GB in %.0fs; building %d indexes\n"
                     % (heap / 1e9, time.time() - t0, len(indexes)))
    for stmt in indexes:
        ti = time.time()
        db.execute(stmt)
        sys.stderr.write("  %s (%.0fs)\n" % (stmt.split('"')[1], time.time() - ti))
        sys.stderr.flush()
    db.commit()
    db.execute("ANALYZE")
    db.commit()
    db.close()
    print(json.dumps({
        "schema": args.schema, "path": args.out,
        "version_mode": args.version_mode, "text_entropy": args.text_entropy,
        "bytes": os.path.getsize(args.out),
        "heap_bytes": heap,
        "page_size": args.page_size,
        "gen_seconds": round(time.time() - t0, 1),
    }))


if __name__ == "__main__":
    main()
