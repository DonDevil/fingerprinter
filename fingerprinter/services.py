"""Wiring: one place that connects every component from ``Settings``."""

from __future__ import annotations

from dataclasses import dataclass

from fingerprinter.commit import Committer
from fingerprinter.config import Settings
from fingerprinter.coord import Coord
from fingerprinter.db import Db, migrate
from fingerprinter.objects import Objects
from fingerprinter.publish import Publisher
from fingerprinter.reconcile import Reconciler
from fingerprinter.requests import Requests
from fingerprinter.store import Store


@dataclass
class Services:
    settings: Settings
    db: Db
    coord: Coord
    objects: Objects
    store: Store
    pub: Publisher
    requests: Requests
    committer: Committer
    reconciler: Reconciler

    @classmethod
    def connect(cls, settings: Settings, *, role: str = "encoder", migrate_schema: bool = False) -> Services:
        db = Db.connect(settings.scylla)
        if migrate_schema:
            migrate(db, settings.scylla.replication_factor)
        coord = Coord.connect(settings.redis)
        objects = Objects.connect(settings.minio)
        store = Store(db, coord, settings, role=role)
        pub = Publisher(store)
        requests = Requests(store, pub)
        committer = Committer(store, requests, objects)
        reconciler = Reconciler(store, requests, committer, objects)
        services = cls(settings, db, coord, objects, store, pub, requests, committer, reconciler)
        from fingerprinter.targets import TargetManager

        TargetManager(services).install()
        return services

    def close(self) -> None:
        self.db.close()
        self.coord.client.close()
