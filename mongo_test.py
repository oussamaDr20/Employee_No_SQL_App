from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import OperationFailure, ConnectionFailure, PyMongoError
from bson.objectid import ObjectId
from datetime import datetime
import re
import pprint
from typing import Dict, Any, List, Optional

#VERBINDUNG & DB-ERSTELLUNG
try:
    client = MongoClient('mongodb://localhost:27017/',
                         serverSelectionTimeoutMS=5000,
                         socketTimeoutMS=30000,
                         connectTimeoutMS=30000)
    client.admin.command('ping') # Test the connection
    print("✅ Erfolgreich mit MongoDB verbunden")

    db = client['unternehmenDB'] # Using a new DB name for normalized schema

    #Schema-Validierung für Mitarbeiter
    mitarbeiter_schema = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['name', 'vorname', 'geburtsdatum', 'stelle', 'gehalt', 'abteilung_id'],
            'properties': {
                'name': {'bsonType': 'string', 'minLength': 2, 'description': 'Name des Mitarbeiters'},
                'vorname': {'bsonType': 'string', 'minLength': 2, 'description': 'Vorname des Mitarbeiters'},
                'geburtsdatum': {'bsonType': 'date', 'description': 'Geburtsdatum des Mitarbeiters'},
                'stelle': {'bsonType': 'string', 'description': 'Position oder Rolle des Mitarbeiters'},
                'gehalt': {
                    'bsonType': 'double',
                    'minimum': 0,
                    'exclusiveMinimum': True,
                    'description': 'Gehalt des Mitarbeiters (muss positiv sein)'
                },
                'abteilung_id': {'bsonType': 'objectId', 'description': 'Referenz zur Abteilung'},
                'einstellungsdatum': {'bsonType': 'date', 'description': 'Datum der Einstellung'},
                'aktualisierung': {'bsonType': 'date', 'description': 'Datum der letzten Aktualisierung'}
            }
        }
    }

    #Schema-Validierung für Abteilung
    abteilung_schema = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['abteilungsname'],
            'properties': {
                'abteilungsname': {'bsonType': 'string', 'minLength': 2, 'description': 'Name der Abteilung'},
                # Korrektur: Ersetze 'nullable': True durch ['objectId', 'null']
                'manager_id': {'bsonType': ['objectId', 'null'], 'description': 'Referenz zum Manager (Mitarbeiter)'}
            }
        }
    }

    #Schema-Validierung für Proj
    projekt_schema = {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['projekt_name', 'beschreibung', 'start_datum', 'end_datum'],
            'properties': {
                'projekt_name': {'bsonType': 'string', 'minLength': 2, 'description': 'Name des Projekts'},
                'beschreibung': {'bsonType': 'string', 'description': 'Beschreibung des Projekts'},
                'start_datum': {'bsonType': 'date', 'description': 'Startdatum des Projekts'},
                'end_datum': {'bsonType': 'date', 'description': 'Enddatum des Projekts'},
                'mitarbeiter_ids': {
                    'bsonType': 'array',
                    'items': {'bsonType': 'objectId'},
                    'description': 'Liste der Mitarbeiter-IDs, die dem Projekt zugewiesen sind'
                }
            }
        }
    }

    if 'mitarbeiter' not in db.list_collection_names():
        db.create_collection('mitarbeiter', validator=mitarbeiter_schema)
    mitarbeiter_collection = db['mitarbeiter']

    if 'abteilung' not in db.list_collection_names():
        db.create_collection('abteilung', validator=abteilung_schema)
    abteilung_collection = db['abteilung']

    if 'projekt' not in db.list_collection_names():
        db.create_collection('projekt', validator=projekt_schema)
    projekt_collection = db['projekt']

    #Indizes erstellen
    mitarbeiter_collection.create_index([('name', ASCENDING), ('vorname', ASCENDING)])
    mitarbeiter_collection.create_index([('gehalt', DESCENDING)])
    mitarbeiter_collection.create_index([('abteilung_id', ASCENDING)])
    mitarbeiter_collection.create_index([('stelle', ASCENDING)])
    abteilung_collection.create_index([('abteilungsname', ASCENDING)], unique=True)
    projekt_collection.create_index([('projekt_name', ASCENDING)], unique=True)

except ConnectionFailure as e:
    print(f"❌ MongoDB-Verbindungsfehler: {e}")
    print("Bitte stellen Sie sicher, dass MongoDB läuft und als Replica Set initialisiert ist.")
    exit()
except OperationFailure as e:
    print(f"❌ MongoDB-Betriebsfehler: {e.code}:{e.details}")
    print("Möglicherweise ein Problem mit der Schema-Validierung oder Indexerstellung.")
    print("Wenn Sie das Schema geändert haben, müssen Sie möglicherweise die betroffenen Collections löschen und neu erstellen.")
    exit()
except PyMongoError as e:
    print(f"❌ Ein allgemeiner PyMongo-Fehler ist aufgetreten: {e}")
    exit()
except Exception as e:
    print(f"❌ Ein unerwarteter Fehler beim Start ist aufgetreten: {e}")
    exit()

# --- Hilfsfunktionen ---

def get_abteilung_name(abteilung_id: ObjectId) -> str:
    """Hilfsfunktion, um den Abteilungsnamen anhand der ID abzurufen."""
    if not ObjectId.is_valid(str(abteilung_id)):
        return "Ungültige Abteilungs-ID"
    abteilung = abteilung_collection.find_one({'_id': abteilung_id}, {'abteilungsname': 1})
    return abteilung['abteilungsname'] if abteilung else "Unbekannt"

def anzeigen_mitarbeiter(emp: Dict[str, Any], index: Optional[int] = None) -> None:
    """Formatiert und zeigt einen Mitarbeiter mit klarer Abgrenzung an."""
    # Stellen Sie sicher, dass emp ein Diktat ist und die erforderlichen Schlüssel hat
    if not isinstance(emp, dict):
        print("Fehler: Ungültiges Mitarbeiterobjekt zur Anzeige.")
        return

    mitarbeiter_id = str(emp.get('_id', 'N/A'))
    einstellung = emp.get('einstellungsdatum')
    einstellung_str = einstellung.strftime('%d/%m/%Y') if isinstance(einstellung, datetime) else "Nicht angegeben"
    aktualisierung = emp.get('aktualisierung')
    aktualisierung_str = aktualisierung.strftime('%d/%m/%Y') if isinstance(aktualisierung, datetime) else "Nicht angegeben"
    geburtsdatum = emp.get('geburtsdatum')
    geburtsdatum_str = geburtsdatum.strftime('%d/%m/%Y') if isinstance(geburtsdatum, datetime) else "Nicht angegeben"
    stelle = emp.get('stelle', 'Nicht angegeben')
    gehalt = emp.get('gehalt', 0.0)

    # Abteilungsname abrufen, entweder direkt aus dem Dokument (wenn aggregiert) oder über die ID
    abteilung_name = emp.get('abteilung_name') # Für aggregierte Ergebnisse
    if not abteilung_name and emp.get('abteilung_id'): # Für nicht-aggregierte Ergebnisse
        abteilung_name = get_abteilung_name(emp['abteilung_id'])
    elif not abteilung_name:
        abteilung_name = "Unbekannt"


    if index is not None:
        print(f"\n\033[1mMITARBEITER #{index + 1}\033[0m")
    print("┌" + "─" * 50 + "┐")
    print(f"│ \033[94m{'ID:':<12}\033[0m {mitarbeiter_id}")
    print(f"│ \033[94m{'Name:':<12}\033[0m {emp.get('name', 'N/A')}")
    print(f"│ \033[94m{'Vorname:':<12}\033[0m {emp.get('vorname', 'N/A')}")
    print(f"│ \033[94m{'Geburtsdatum:':<12}\033[0m {geburtsdatum_str}")
    print(f"│ \033[94m{'Stelle:':<12}\033[0m {stelle}")
    print(f"│ \033[94m{'Gehalt:':<12}\033[0m {gehalt:,.2f} €".replace(",", " "))
    print(f"│ \033[94m{'Abteilung:':<12}\033[0m {abteilung_name}")
    print(f"│ \033[94m{'Einstellung:':<12}\033[0m {einstellung_str}")
    print(f"│ \033[94m{'Letzte Aktualisierung:':<12}\033[0m {aktualisierung_str}")
    print("└" + "─" * 50 + "┘")

# --- CRUD-Operationen für Mitarbeiter ---

def hinzufuegen_mitarbeiter() -> None:
    """Fügt einen neuen Mitarbeiter zur Datenbank hinzu."""
    print("\n--- MITARBEITER HINZUFÜGEN ---")
    try:
        name = input("Name: ").strip()
        if not re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ\- ]+$", name):
            raise ValueError("Ungültiger Name. Nur Buchstaben, Bindestriche und Leerzeichen erlaubt.")

        vorname = input("Vorname: ").strip()
        if not re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ\- ]+$", vorname):
            raise ValueError("Ungültiger Vorname. Nur Buchstaben, Bindestriche und Leerzeichen erlaubt.")

        geburtsdatum_str = input("Geburtsdatum (JJJJ-MM-TT): ").strip()
        try:
            geburtsdatum = datetime.strptime(geburtsdatum_str, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Ungültiges Datumsformat für Geburtsdatum. Bitte JJJJ-MM-TT verwenden.")

        stelle = input("Stelle: ").strip()
        if not stelle:
            raise ValueError("Stelle darf nicht leer sein.")

        gehalt_str = input("Gehalt: ").strip()
        try:
            gehalt = float(gehalt_str)
            if gehalt <= 0:
                raise ValueError("Gehalt muss positiv sein.")
        except ValueError:
            raise ValueError("Ungültiges Gehalt. Bitte eine Zahl eingeben.")

        # Abteilung auswählen
        abteilungen = list(abteilung_collection.find({}, {'abteilungsname': 1}))
        if not abteilungen:
            print("⚠️ Keine Abteilungen gefunden. Bitte zuerst eine Abteilung hinzufügen.")
            return

        print("\nVerfügbare Abteilungen:")
        for i, dept in enumerate(abteilungen):
            print(f"{i+1}. {dept['abteilungsname']}")
        
        abteilung_wahl_str = input("Wählen Sie eine Abteilung (Nummer): ").strip()
        try:
            abteilung_wahl = int(abteilung_wahl_str) - 1
            if not (0 <= abteilung_wahl < len(abteilungen)):
                raise ValueError("Ungültige Abteilungswahl.")
            abteilung_id = abteilungen[abteilung_wahl]['_id']
        except ValueError as ve:
            raise ValueError(f"Ungültige Eingabe für Abteilungswahl: {ve}")

        mitarbeiter_data = {
            "name": name,
            "vorname": vorname,
            "geburtsdatum": geburtsdatum,
            "stelle": stelle,
            "gehalt": gehalt,
            "abteilung_id": abteilung_id,
            "einstellungsdatum": datetime.now(),
            "aktualisierung": datetime.now()
        }

        result = mitarbeiter_collection.insert_one(mitarbeiter_data)
        print(f"\n✅ Mitarbeiter hinzugefügt mit der ID: {result.inserted_id}")
    except ValueError as ve:
        print(f"❌ Eingabefehler: {ve}")
    except OperationFailure as e:
        print(f"❌ MongoDB-Fehler beim Hinzufügen des Mitarbeiters: {e.code}:{e.details}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")

def auflisten_mitarbeiter() -> None:
    """Listet alle Mitarbeiter mit klarer Abgrenzung auf, inklusive Abteilungsname."""
    print("\n\033[1m╔═════════════════════════════════════╗")
    print("║        MITARBEITERLISTE           ║")
    print("╚═════════════════════════════════════╝\033[0m")

    # Aggregation Pipeline für JOIN-ähnliche Operation
    pipeline = [
        {
            '$lookup': {
                'from': 'abteilung',
                'localField': 'abteilung_id',
                'foreignField': '_id',
                'as': 'abteilung_info'
            }
        },
        {
            '$unwind': {
                'path': '$abteilung_info',
                'preserveNullAndEmptyArrays': True # Behält Mitarbeiter ohne zugeordnete Abteilung
            }
        },
        {
            '$project': {
                '_id': 1,
                'name': 1,
                'vorname': 1,
                'geburtsdatum': 1,
                'stelle': 1,
                'gehalt': 1,
                'einstellungsdatum': 1,
                'aktualisierung': 1,
                'abteilung_id': 1, # Behalten für die Anzeige, falls abteilung_info leer ist
                'abteilung_name': {'$ifNull': ['$abteilung_info.abteilungsname', 'Unbekannt']}
            }
        },
        {
            '$sort': {'name': ASCENDING, 'vorname': ASCENDING}
        }
    ]

    try:
        alle_mitarbeiter = list(mitarbeiter_collection.aggregate(pipeline))

        if not alle_mitarbeiter:
            print("\n⚠️ Kein Mitarbeiter in der Datenbank gefunden.")
            return

        for i, emp in enumerate(alle_mitarbeiter):
            anzeigen_mitarbeiter(emp, i)
            print() # Zusätzliche Leerzeile pour Abgrenzung
    except PyMongoError as e:
        print(f"❌ Fehler beim Abrufen der Mitarbeiterliste: {e}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")

def aktualisieren_mitarbeiter() -> None:
    """Aktualisiert die Informationen eines Mitarbeiters."""
    print("\n--- MITARBEITER AKTUALISIEREN ---")

    try:
        identifikation = input("Geben Sie den Namen, Vornamen oder die ID des Mitarbeiters ein: ").strip()

        query = {}
        if ObjectId.is_valid(identifikation):
            query['_id'] = ObjectId(identifikation)
        else:
            query['$or'] = [
                {'name': {'$regex': identifikation, '$options': 'i'}},
                {'vorname': {'$regex': identifikation, '$options': 'i'}}
            ]

        gefundene_mitarbeiter = list(mitarbeiter_collection.find(query))

        if not gefundene_mitarbeiter:
            print("❌ Kein Mitarbeiter gefunden.")
            return

        mitarbeiter = None
        if len(gefundene_mitarbeiter) > 1:
            print("\nMehrere Mitarbeiter gefunden:")
            for i, emp in enumerate(gefundene_mitarbeiter, 1):
                abteilung_name = get_abteilung_name(emp.get('abteilung_id'))
                print(f"{i}. {emp.get('vorname', 'N/A')} {emp.get('name', 'N/A')} (ID: {emp.get('_id', 'N/A')}) - {emp.get('stelle', 'N/A')} - {abteilung_name}")
            while True:
                try:
                    wahl = int(input("Wählen Sie einen Mitarbeiter (Nummer): ")) - 1
                    if 0 <= wahl < len(gefundene_mitarbeiter):
                        mitarbeiter = gefundene_mitarbeiter[wahl]
                        break
                    print("❌ Ungültige Nummer.")
                except ValueError:
                    print("❌ Bitte geben Sie eine Zahl ein.")
        else:
            mitarbeiter = gefundene_mitarbeiter[0]

        if not mitarbeiter: # Should not happen if gefundene_mitarbeiter is not empty
            print("Fehler: Mitarbeiter konnte nicht ausgewählt werden.")
            return

        print("\nAusgewählter Mitarbeiter:")
        anzeigen_mitarbeiter(mitarbeiter)

        while True:
            current_abteilung_name = get_abteilung_name(mitarbeiter.get('abteilung_id'))
            print("\nÄnderbare Felder:")
            print("1. Name           (aktuell:", mitarbeiter.get('name', ''))
            print("2. Vorname        (aktuell:", mitarbeiter.get('vorname', ''))
            print("3. Geburtsdatum   (aktuell:", mitarbeiter.get('geburtsdatum', '').strftime('%Y-%m-%d') if isinstance(mitarbeiter.get('geburtsdatum'), datetime) else '')
            print("4. Stelle         (aktuell:", mitarbeiter.get('stelle', ''))
            print("5. Gehalt         (aktuell:", mitarbeiter.get('gehalt', ''))
            print("6. Abteilung      (aktuell:", current_abteilung_name)
            print("7. Beenden")

            wahl = input("\nWas möchten Sie ändern? (1-7): ").strip()

            if wahl == "7":
                break

            feld = None
            wert = None
            update_data = {'$set': {'aktualisierung': datetime.now()}}

            if wahl == "1":
                feld = 'name'
                wert = input(f"Neuer Name ({mitarbeiter['name']}): ").strip()
                if not wert or wert == mitarbeiter['name']:
                    print("Keine Änderung.")
                    continue
                if not re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ\- ]+$", wert):
                    print("Ungültiger Name. Nur Buchstaben, Bindestriche und Leerzeichen erlaubt.")
                    continue

            elif wahl == "2":
                feld = 'vorname'
                wert = input(f"Neuer Vorname ({mitarbeiter['vorname']}): ").strip()
                if not wert or wert == mitarbeiter['vorname']:
                    print("Keine Änderung.")
                    continue
                if not re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ\- ]+$", wert):
                    print("Ungültiger Vorname. Nur Buchstaben, Bindestriche und Leerzeichen erlaubt.")
                    continue

            elif wahl == "3":
                feld = 'geburtsdatum'
                wert_str = input(f"Neues Geburtsdatum ({mitarbeiter.get('geburtsdatum', '').strftime('%Y-%m-%d') if isinstance(mitarbeiter.get('geburtsdatum'), datetime) else ''}) (JJJJ-MM-TT): ").strip()
                if not wert_str:
                    print("Keine Änderung.")
                    continue
                try:
                    wert = datetime.strptime(wert_str, '%Y-%m-%d')
                    if wert == mitarbeiter.get('geburtsdatum'):
                        print("Keine Änderung.")
                        continue
                except ValueError:
                    print("❌ Ungültiges Datumsformat. Bitte JJJJ-MM-TT verwenden.")
                    continue

            elif wahl == "4":
                feld = 'stelle'
                wert = input(f"Neue Stelle ({mitarbeiter.get('stelle', '')}): ").strip()
                if not wert or ('stelle' in mitarbeiter and wert == mitarbeiter['stelle']):
                    print("Keine Änderung.")
                    continue

            elif wahl == "5":
                feld = 'gehalt'
                wert_str = input(f"Neues Gehalt ({mitarbeiter['gehalt']}): ").strip()
                if not wert_str:
                    print("Keine Änderung.")
                    continue
                try:
                    wert = float(wert_str)
                    if wert <= 0:
                        print("Das Gehalt muss positif sein.")
                        continue
                    if wert == mitarbeiter['gehalt']:
                        print("Keine Änderung.")
                        continue
                except ValueError:
                    print("Bitte geben Sie eine gültige Zahl ein.")
                    continue

            elif wahl == "6":
                feld = 'abteilung_id'
                abteilungen = list(abteilung_collection.find({}, {'abteilungsname': 1}))
                if not abteilungen:
                    print("⚠️ Keine Abteilungen zum Wechseln vorhanden.")
                    continue
                print("\nVerfügbare Abteilungen:")
                for i, dept in enumerate(abteilungen):
                    print(f"{i+1}. {dept['abteilungsname']}")
                abteilung_wahl_str = input("Wählen Sie eine neue Abteilung (Nummer): ").strip()
                try:
                    abteilung_wahl_idx = int(abteilung_wahl_str) - 1
                    if 0 <= abteilung_wahl_idx < len(abteilungen):
                        neue_abteilung_id = abteilungen[abteilung_wahl_idx]['_id']
                        if neue_abteilung_id == mitarbeiter.get('abteilung_id'):
                            print("Keine Änderung (gleiche Abteilung).")
                            continue
                        wert = neue_abteilung_id
                    else:
                        print("❌ Ungültige Abteilungswahl.")
                        continue
                except ValueError:
                    print("❌ Bitte geben Sie eine Zahl ein.")
                    continue
            else:
                print("Ungültige Wahl.")
                continue

            if feld and wert is not None: # Ensure both field and value are set
                update_data['$set'][feld] = wert

                try:
                    result = mitarbeiter_collection.update_one(
                        {'_id': mitarbeiter['_id']},
                        update_data
                    )

                    if result.modified_count == 1:
                        print(f"✅ {feld.capitalize()} erfolgreich aktualisiert.")
                        mitarbeiter[feld] = wert  # Lokal aktualisieren pour die nächste Iteration
                    else:
                        print("⚠️ Keine Änderungen vorgenommen (Daten identisch oder Fehler).")

                except OperationFailure as e:
                    print(f"❌ MongoDB-Fehler beim Aktualisieren: {e.details}")
                except Exception as e:
                    print(f"❌ Ein unerwarteter Fehler beim Aktualisieren ist aufgetreten: {e}")
            else:
                print("Keine gültige Eingabe pour die Aktualisierung.")

    except PyMongoError as e:
        print(f"❌ Fehler beim Suchen oder Aktualisieren des Mitarbeiters: {e}")
    except Exception as e:
        print(f"❌ Unerwarteter Fehler: {str(e)}")

def loeschen_mitarbeiter() -> None:
    """Löscht einen Mitarbeiter aus der Datenbank."""
    print("\n--- MITARBEITER LÖSCHEN ---")
    try:
        identifikation = input("Geben Sie den Namen, Vornamen oder die ID des zu löschenden Mitarbeiters ein: ").strip()

        query = {}
        if ObjectId.is_valid(identifikation):
            query['_id'] = ObjectId(identifikation)
        else:
            query['$or'] = [
                {'name': {'$regex': identifikation, '$options': 'i'}},
                {'vorname': {'$regex': identifikation, '$options': 'i'}}
            ]

        # Find the employee first to confirm
        mitarbeiter_to_delete = mitarbeiter_collection.find_one(query)
        if not mitarbeiter_to_delete:
            print("❌ Kein Mitarbeiter mit diesem Namen/Vornamen/ID gefunden.")
            return

        anzeigen_mitarbeiter(mitarbeiter_to_delete)
        confirm = input("Sind Sie sicher, dass Sie diesen Mitarbeiter löschen möchten? (ja/nein): ").lower().strip()
        if confirm != 'ja':
            print("Löschvorgang abgebrochen.")
            return

        result = mitarbeiter_collection.delete_one({'_id': mitarbeiter_to_delete['_id']})

        if result.deleted_count > 0:
            print("✅ Mitarbeiter erfolgreich gelöscht.")
        else:
            print("❌ Fehler beim Löschen des Mitarbeiters.") # Should not happen if find_one was successful

    except PyMongoError as e:
        print(f"❌ Fehler beim Löschen: {e}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {str(e)}")

# --- CRUD-Operationen für Abteilungen ---

def hinzufuegen_abteilung() -> None:
    """Fügt eine neue Abteilung hinzu."""
    print("\n--- ABTEILUNG HINZUFÜGEN ---")
    try:
        abteilungsname = input("Name der Abteilung: ").strip()  # Correction here
        if not abteilungsname:
            raise ValueError("Abteilungsname darf nicht leer sein.")

        # Überprüfen, ob die Abteilung bereits existiert (case-insensitive)
        if abteilung_collection.find_one({'abteilungsname': {'$regex': f"^{re.escape(abteilungsname)}$", '$options': 'i'}}):
            print(f"⚠️ Abteilung '{abteilungsname}' existiert bereits.")
            return

        result = abteilung_collection.insert_one({
            "abteilungsname": abteilungsname,
            "manager_id": None # Initial kein Manager
        })
        print(f"\n✅ Abteilung '{abteilungsname}' hinzugefügt mit der ID: {result.inserted_id}")
    except ValueError as ve:
        print(f"❌ Eingabefehler: {ve}")
    except OperationFailure as e:
        print(f"❌ MongoDB-Fehler beim Hinzufügen der Abteilung: {e.code}:{e.details}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")

def setze_abteilungsmanager() -> None:
    """Setzt oder aktualisiert den Manager einer Abteilung ohne Transaktion."""
    print("\n--- ABTEILUNGSMANAGER ZUWEISEN ---")
    try:
        # Abteilungen auflisten
        abteilungen = list(abteilung_collection.find())
        if not abteilungen:
            print("⚠️ Keine Abteilungen zum Zuweisen eines Managers vorhanden.")
            return

        print("\nVerfügbare Abteilungen:")
        for i, dept in enumerate(abteilungen):
            manager_info = "Kein Manager"
            if dept.get('manager_id') and ObjectId.is_valid(str(dept['manager_id'])):
                manager = mitarbeiter_collection.find_one({'_id': dept['manager_id']}, {'name': 1, 'vorname': 1})
                if manager:
                    manager_info = f"{manager.get('vorname', 'N/A')} {manager.get('name', 'N/A')}"
            print(f"{i+1}. {dept['abteilungsname']} (Aktueller Manager: {manager_info})")

        abteilung_wahl_str = input("Wählen Sie eine Abteilung (Nummer): ").strip()
        try:
            abteilung_wahl = int(abteilung_wahl_str) - 1
            if not (0 <= abteilung_wahl < len(abteilungen)):
                raise ValueError("Ungültige Abteilungswahl.")
            gewaehlte_abteilung = abteilungen[abteilung_wahl]
            abteilung_id = gewaehlte_abteilung['_id']
        except ValueError as ve:
            raise ValueError(f"Ungültige Eingabe für Abteilungswahl: {ve}")

        # Mitarbeiter auflisten, die als Manager in Frage kommen
        mitarbeiter_liste = list(mitarbeiter_collection.find({}, {'name': 1, 'vorname': 1, 'stelle': 1}))
        if not mitarbeiter_liste:
            print("⚠️ Keine Mitarbeiter zum Zuweisen als Manager vorhanden.")
            return

        print("\nVerfügbare Mitarbeiter:")
        for i, emp in enumerate(mitarbeiter_liste):
            print(f"{i+1}. {emp.get('vorname', 'N/A')} {emp.get('name', 'N/A')} ({emp.get('stelle', 'N/A')})")

        mitarbeiter_wahl_str = input("Wählen Sie einen Mitarbeiter als Manager (Nummer, oder '0' pour keinen Manager): ").strip()
        try:
            mitarbeiter_wahl = int(mitarbeiter_wahl_str) - 1
            if mitarbeiter_wahl_str == '0':
                manager_id = None
                gewaehlter_manager = None
            elif not (0 <= mitarbeiter_wahl < len(mitarbeiter_liste)):
                raise ValueError("Ungültige Mitarbeiterwahl.")
            else:
                gewaehlter_manager = mitarbeiter_liste[mitarbeiter_wahl]
                manager_id = gewaehlter_manager['_id']
        except ValueError as ve:
            raise ValueError(f"Ungültige Eingabe für Mitarbeiterwahl: {ve}")

        # Ohne Transaktion - sequenzielle Updates mit Fehlerbehandlung
        try:
            # 1. Alten Manager (falls vorhanden und nicht der neue Manager) seine Manager-Stelle entziehen
            alter_manager_id = gewaehlte_abteilung.get('manager_id')
            if alter_manager_id and ObjectId.is_valid(str(alter_manager_id)) and alter_manager_id != manager_id:
                result_old = mitarbeiter_collection.update_one(
                    {'_id': alter_manager_id},
                    {'$set': {'stelle': 'Mitarbeiter', 'aktualisierung': datetime.now()}}
                )
                if result_old.modified_count > 0:
                    print(f"✅ Alter Manager (ID: {alter_manager_id}) wurde auf 'Mitarbeiter' aktualisiert.")
                else:
                    print(f"⚠️ Warnung: Alter Manager konnte nicht aktualisiert werden.")

            # 2. Neuen Manager seine Stelle zuweisen (falls ein Manager gewählt wurde)
            if manager_id:
                result_new = mitarbeiter_collection.update_one(
                    {'_id': manager_id},
                    {'$set': {'stelle': 'Manager', 'aktualisierung': datetime.now()}}
                )
                if result_new.modified_count > 0:
                    print(f"✅ Mitarbeiter {gewaehlter_manager.get('vorname', 'N/A')} {gewaehlter_manager.get('name', 'N/A')} als 'Manager' aktualisiert.")
                else:
                    print(f"⚠️ Warnung: Neuer Manager konnte nicht aktualisiert werden.")

            # 3. Abteilung mit neuem Manager aktualisieren
            result_dept = abteilung_collection.update_one(
                {'_id': abteilung_id},
                {'$set': {'manager_id': manager_id}}
            )
            if result_dept.modified_count > 0:
                print(f"✅ Abteilung '{gewaehlte_abteilung['abteilungsname']}' Manager zugewiesen.")
            else:
                print(f"⚠️ Warnung: Abteilung konnte nicht aktualisiert werden.")

            print("\n✅ Manager-Zuweisung abgeschlossen.")

        except OperationFailure as e:
            print(f"❌ MongoDB-Fehler während der Manager-Zuweisung: {e.code}:{e.details}")
        except Exception as e:
            print(f"❌ Fehler während der Manager-Zuweisung: {e}")

    except ValueError as ve:
        print(f"❌ Eingabefehler: {ve}")
    except PyMongoError as e:
        print(f"❌ Ein PyMongo-Fehler ist aufgetreten: {e}")
    except Exception as e:
        print(f"❌ Unerwarteter Fehler: {e}")


def setze_abteilungsmanager_mit_replica_set() -> None:
    """
    Alternative Version mit Transaktionen - nur verwenden wenn MongoDB als Replica Set läuft.
    
    Um MongoDB als Replica Set zu konfigurieren:
    1. Stoppen Sie MongoDB
    2. Starten Sie MongoDB mit: mongod --replSet rs0 --dbpath /data/db
    3. Verbinden Sie sich mit mongo shell: mongo
    4. Initialisieren Sie das Replica Set: rs.initiate()
    5. Warten Sie bis rs.status() "PRIMARY" anzeigt
    """
    print("\n--- ABTEILUNGSMANAGER ZUWEISEN (MIT TRANSAKTION) ---")
    session = None
    try:
        # Prüfen ob Replica Set verfügbar ist
        try:
            client.admin.command('isMaster')
            replica_set_status = client.admin.command('replSetGetStatus')
            if not replica_set_status.get('ok'):
                raise Exception("Kein Replica Set aktiv")
        except Exception:
            print("❌ Diese Funktion erfordert ein MongoDB Replica Set.")
            print("Verwenden Sie stattdessen die normale Manager-Zuweisung (Option 6).")
            return

        # Abteilungen auflisten
        abteilungen = list(abteilung_collection.find())
        if not abteilungen:
            print("⚠️ Keine Abteilungen zum Zuweisen eines Managers vorhanden.")
            return

        print("\nVerfügbare Abteilungen:")
        for i, dept in enumerate(abteilungen):
            manager_info = "Kein Manager"
            if dept.get('manager_id') and ObjectId.is_valid(str(dept['manager_id'])):
                manager = mitarbeiter_collection.find_one({'_id': dept['manager_id']}, {'name': 1, 'vorname': 1})
                if manager:
                    manager_info = f"{manager.get('vorname', 'N/A')} {manager.get('name', 'N/A')}"
            print(f"{i+1}. {dept['abteilungsname']} (Aktueller Manager: {manager_info})")

        abteilung_wahl_str = input("Wählen Sie eine Abteilung (Nummer): ").strip()
        try:
            abteilung_wahl = int(abteilung_wahl_str) - 1
            if not (0 <= abteilung_wahl < len(abteilungen)):
                raise ValueError("Ungültige Abteilungswahl.")
            gewaehlte_abteilung = abteilungen[abteilung_wahl]
            abteilung_id = gewaehlte_abteilung['_id']
        except ValueError as ve:
            raise ValueError(f"Ungültige Eingabe für Abteilungswahl: {ve}")

        # Mitarbeiter auflisten
        mitarbeiter_liste = list(mitarbeiter_collection.find({}, {'name': 1, 'vorname': 1, 'stelle': 1}))
        if not mitarbeiter_liste:
            print("⚠️ Keine Mitarbeiter zum Zuweisen als Manager vorhanden.")
            return

        print("\nVerfügbare Mitarbeiter:")
        for i, emp in enumerate(mitarbeiter_liste):
            print(f"{i+1}. {emp.get('vorname', 'N/A')} {emp.get('name', 'N/A')} ({emp.get('stelle', 'N/A')})")

        mitarbeiter_wahl_str = input("Wählen Sie einen Mitarbeiter als Manager (Nummer, oder '0' pour keinen Manager): ").strip()
        try:
            mitarbeiter_wahl = int(mitarbeiter_wahl_str) - 1
            if mitarbeiter_wahl_str == '0':
                manager_id = None
                gewaehlter_manager = None
            elif not (0 <= mitarbeiter_wahl < len(mitarbeiter_liste)):
                raise ValueError("Ungültige Mitarbeiterwahl.")
            else:
                gewaehlter_manager = mitarbeiter_liste[mitarbeiter_wahl]
                manager_id = gewaehlter_manager['_id']
        except ValueError as ve:
            raise ValueError(f"Ungültige Eingabe für Mitarbeiterwahl: {ve}")

        # Transaktion starten
        session = client.start_session()
        session.start_transaction()

        try:
            # 1. Alten Manager zurücksetzen
            alter_manager_id = gewaehlte_abteilung.get('manager_id')
            if alter_manager_id and ObjectId.is_valid(str(alter_manager_id)) and alter_manager_id != manager_id:
                mitarbeiter_collection.update_one(
                    {'_id': alter_manager_id},
                    {'$set': {'stelle': 'Mitarbeiter', 'aktualisierung': datetime.now()}},
                    session=session
                )
                print(f"✅ Alter Manager (ID: {alter_manager_id}) wurde auf 'Mitarbeiter' aktualisiert.")

            # 2. Neuen Manager setzen
            if manager_id:
                mitarbeiter_collection.update_one(
                    {'_id': manager_id},
                    {'$set': {'stelle': 'Manager', 'aktualisierung': datetime.now()}},
                    session=session
                )
                print(f"✅ Mitarbeiter {gewaehlter_manager.get('vorname', 'N/A')} {gewaehlter_manager.get('name', 'N/A')} als 'Manager' aktualisiert.")

            # 3. Abteilung aktualisieren
            abteilung_collection.update_one(
                {'_id': abteilung_id},
                {'$set': {'manager_id': manager_id}},
                session=session
            )
            print(f"✅ Abteilung '{gewaehlte_abteilung['abteilungsname']}' Manager zugewiesen.")

            session.commit_transaction()
            print("\n✅ Manager erfolgreich zugewiesen (Transaktion abgeschlossen).")

        except Exception as e:
            session.abort_transaction()
            print(f"❌ Transaktion fehlgeschlagen: {e}. Änderungen wurden zurückgerollt.")
            raise

    except ValueError as ve:
        print(f"❌ Eingabefehler: {ve}")
    except OperationFailure as e:
        print(f"❌ MongoDB-Betriebsfehler während der Transaktion: {e.code}:{e.details}")
        print("Stellen Sie sicher, dass MongoDB als Replica Set läuft.")
    except PyMongoError as e:
        print(f"❌ Ein PyMongo-Fehler ist aufgetreten: {e}")
    except Exception as e:
        print(f"❌ Unerwarteter Fehler: {e}")
    finally:
        if session:
            session.end_session()

# --- CRUD-Operationen für Projekte ---

def hinzufuegen_projekt() -> None:
    """Fügt ein neues Projekt hinzu."""
    print("\n--- PROJEKT HINZUFÜGEN ---")
    try:
        projekt_name = input("Projektname: ").strip()
        if not projekt_name:
            raise ValueError("Projektname darf nicht leer sein.")

        # Überprüfen, ob Projektname bereits existiert
        if projekt_collection.find_one({'projekt_name': {'$regex': f"^{re.escape(projekt_name)}$", '$options': 'i'}}):
            print(f"⚠️ Projekt '{projekt_name}' existiert bereits.")
            return

        beschreibung = input("Beschreibung: ").strip()
        if not beschreibung:
            raise ValueError("Beschreibung darf nicht leer sein.")

        start_datum_str = input("Startdatum (JJJJ-MM-TT): ").strip()
        try:
            start_datum = datetime.strptime(start_datum_str, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Ungültiges Datumsformat für Startdatum. Bitte JJJJ-MM-TT verwenden.")

        end_datum_str = input("Enddatum (JJJJ-MM-TT): ").strip()
        try:
            end_datum = datetime.strptime(end_datum_str, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Ungültiges Datumsformat für Enddatum. Bitte JJJJ-MM-TT verwenden.")

        if end_datum < start_datum:
            raise ValueError("Enddatum kann nicht vor Startdatum liegen.")

        projekt_data = {
            "projekt_name": projekt_name,
            "beschreibung": beschreibung,
            "start_datum": start_datum,
            "end_datum": end_datum,
            "mitarbeiter_ids": [] # Initial keine Mitarbeiter zugewiesen
        }

        result = projekt_collection.insert_one(projekt_data)
        print(f"\n✅ Projekt '{projekt_name}' hinzugefügt mit der ID: {result.inserted_id}")
    except ValueError as ve:
        print(f"❌ Eingabefehler: {ve}")
    except OperationFailure as e:
        print(f"❌ MongoDB-Fehler beim Hinzufügen des Projekts: {e.code}:{e.details}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")

def auflisten_projekte() -> None:
    """Listet alle Projekte auf, inklusive zugewiesener Mitarbeiter."""
    print("\n\033[1m╔═════════════════════════════════════╗")
    print("║           PROJEKTLISTE            ║")
    print("╚═════════════════════════════════════╝\033[0m")

    pipeline = [
        {
            '$lookup': {
                'from': 'mitarbeiter',
                'localField': 'mitarbeiter_ids',
                'foreignField': '_id',
                'as': 'zugewiesene_mitarbeiter'
            }
        },
        {
            '$project': {
                '_id': 1,
                'projekt_name': 1,
                'beschreibung': 1,
                'start_datum': 1,
                'end_datum': 1,
                'zugewiesene_mitarbeiter_namen': {
                    '$map': {
                        'input': '$zugewiesene_mitarbeiter',
                        'as': 'emp',
                        'in': {'$concat': ['$$emp.vorname', ' ', '$$emp.name']}
                    }
                }
            }
        },
        {'$sort': {'projekt_name': ASCENDING}}
    ]

    try:
        alle_projekte = list(projekt_collection.aggregate(pipeline))

        if not alle_projekte:
            print("\n⚠️ Keine Projekte in der Datenbank gefunden.")
            return

        for i, proj in enumerate(alle_projekte):
            print(f"\n\033[1mPROJEKT #{i + 1}\033[0m")
            print("┌" + "─" * 50 + "┐")
            print(f"│ \033[94m{'ID:':<12}\033[0m {str(proj.get('_id', 'N/A'))}")
            print(f"│ \033[94m{'Name:':<12}\033[0m {proj.get('projekt_name', 'N/A')}")
            print(f"│ \033[94m{'Beschreibung:':<12}\033[0m {proj.get('beschreibung', 'N/A')}")
            start_date_str = proj['start_datum'].strftime('%d/%m/%Y') if isinstance(proj.get('start_datum'), datetime) else "N/A"
            end_date_str = proj['end_datum'].strftime('%d/%m/%Y') if isinstance(proj.get('end_datum'), datetime) else "N/A"
            print(f"│ \033[94m{'Startdatum:':<12}\033[0m {start_date_str}")
            print(f"│ \033[94m{'Enddatum:':<12}\033[0m {end_date_str}")
            mitarbeiter_namen = ", ".join(proj.get('zugewiesene_mitarbeiter_namen', []))
            if not mitarbeiter_namen:
                mitarbeiter_namen = "Keine zugewiesen"
            print(f"│ \033[94m{'Mitarbeiter:':<12}\033[0m {mitarbeiter_namen}")
            print("└" + "─" * 50 + "┘")
    except PyMongoError as e:
        print(f"❌ Fehler beim Abrufen der Projektliste: {e}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")

def zuweisen_mitarbeiter_projekt() -> None:
    """Weist Mitarbeiter einem Projekt zu."""
    print("\n--- MITARBEITER PROJEKT ZUWEISEN ---")
    try:
        projekte = list(projekt_collection.find({}, {'projekt_name': 1}))
        if not projekte:
            print("⚠️ Keine Projekte zum Zuweisen vorhanden.")
            return

        print("\nVerfügbare Projekte:")
        for i, proj in enumerate(projekte):
            print(f"{i+1}. {proj['projekt_name']}")
        projekt_wahl_str = input("Wählen Sie ein Projekt (Nummer): ").strip()
        try:
            projekt_wahl = int(projekt_wahl_str) - 1
            if not (0 <= projekt_wahl < len(projekte)):
                raise ValueError("Ungültige Projektwahl.")
            gewaehltes_projekt_id = projekte[projekt_wahl]['_id']
        except ValueError as ve:
            raise ValueError(f"Ungültige Eingabe für Projektwahl: {ve}")

        mitarbeiter_liste = list(mitarbeiter_collection.find({}, {'name': 1, 'vorname': 1}))
        if not mitarbeiter_liste:
            print("⚠️ Keine Mitarbeiter zum Zuweisen vorhanden.")
            return

        print("\nVerfügbare Mitarbeiter:")
        for i, emp in enumerate(mitarbeiter_liste):
            print(f"{i+1}. {emp.get('vorname', 'N/A')} {emp.get('name', 'N/A')}")

        mitarbeiter_indices_str = input("Geben Sie die Nummern der zuzuweisenden Mitarbeiter ein (durch Komma getrennt, z.B. 1,3,5): ").strip()
        if not mitarbeiter_indices_str:
            print("Keine Mitarbeiter zum Zuweisen ausgewählt.")
            return

        mitarbeiter_ids_to_add = []
        for idx_str in mitarbeiter_indices_str.split(','):
            try:
                idx = int(idx_str.strip()) - 1
                if 0 <= idx < len(mitarbeiter_liste):
                    mitarbeiter_ids_to_add.append(mitarbeiter_liste[idx]['_id'])
                else:
                    print(f"⚠️ Ungültige Mitarbeiternummer: {idx_str}. Wird ignoriert.")
            except ValueError:
                print(f"⚠️ Ungültige Eingabe: '{idx_str}'. Muss eine Zahl sein. Wird ignoriert.")

        if not mitarbeiter_ids_to_add:
            print("Keine gültigen Mitarbeiter zum Zuweisen ausgewählt.")
            return

        # Füge nur neue IDs hinzu, um Duplikate zu vermeiden
        result = projekt_collection.update_one(
            {'_id': gewaehltes_projekt_id},
            {'$addToSet': {'mitarbeiter_ids': {'$each': mitarbeiter_ids_to_add}}}
        )

        if result.modified_count > 0:
            print(f"✅ Mitarbeiter erfolgreich dem Projekt zugewiesen.")
        else:
            print("⚠️ Keine neuen Mitarbeiter zugewiesen (möglicherweise bereits alle zugewiesen).")

    except ValueError as ve:
        print(f"❌ Eingabefehler: {ve}")
    except PyMongoError as e:
        print(f"❌ Fehler beim Zuweisen von Mitarbeitern zum Projekt: {e}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")
#Aggregationsstatistiken

def aggregationsstatistiken() -> None:
    """Zeigt Statistiken über die Mitarbeiter und Abteilungen an."""
    print("\n--- STATISTIKEN ---")

    pipeline_abteilung = [
        {
            '$lookup': {
                'from': 'mitarbeiter',
                'localField': 'manager_id',
                'foreignField': '_id',
                'as': 'manager_info'
            }
        },
        {
            '$unwind': {
                'path': '$manager_info',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$lookup': {
                'from': 'mitarbeiter',
                'localField': '_id',
                'foreignField': 'abteilung_id',
                'as': 'mitarbeiter_in_abteilung'
            }
        },
        {
            '$project': {
                '_id': 0,
                'Abteilung': '$abteilungsname',
                'Manager': {
                    '$cond': {
                        'if': '$manager_info',
                        'then': {'$concat': ['$manager_info.vorname', ' ', '$manager_info.name']},
                        'else': 'Kein Manager zugewiesen'
                    }
                },
                'Anzahl_Mitarbeiter': {'$size': '$mitarbeiter_in_abteilung'},
                'Durchschnittsgehalt_Abteilung': {'$avg': '$mitarbeiter_in_abteilung.gehalt'},
                'Max_Gehalt_Abteilung': {'$max': '$mitarbeiter_in_abteilung.gehalt'},
                'Min_Gehalt_Abteilung': {'$min': '$mitarbeiter_in_abteilung.gehalt'}
            }
        },
        {'$sort': {'Abteilung': ASCENDING}}
    ]

    print("\nStatistiken nach Abteilung:")
    try:
        for stat in abteilung_collection.aggregate(pipeline_abteilung):
            stat['Durchschnittsgehalt_Abteilung'] = round(stat['Durchschnittsgehalt_Abteilung'], 2) if stat['Durchschnittsgehalt_Abteilung'] is not None else 'N/A'
            stat['Max_Gehalt_Abteilung'] = round(stat['Max_Gehalt_Abteilung'], 2) if stat['Max_Gehalt_Abteilung'] is not None else 'N/A'
            stat['Min_Gehalt_Abteilung'] = round(stat['Min_Gehalt_Abteilung'], 2) if stat['Min_Gehalt_Abteilung'] is not None else 'N/A'
            pprint.pprint(stat)
    except PyMongoError as e:
        print(f"❌ Fehler beim Abrufen der Abteilungsstatistiken: {e}")

    # Globale Gehaltsstatistiken
    pipeline_global_gehalt = [
        {
            '$group': {
                '_id': None, # Für globale Aggregation
                'Gesamt_Mitarbeiter': {'$sum': 1},
                'Durchschnittsgehalt_Gesamt': {'$avg': '$gehalt'},
                'Max_Gehalt_Gesamt': {'$max': '$gehalt'},
                'Min_Gehalt_Gesamt': {'$min': '$gehalt'}
            }
        }
    ]
    print("\nGlobale Gehaltsstatistiken:")
    try:
        global_stat = list(mitarbeiter_collection.aggregate(pipeline_global_gehalt))
        if global_stat:
            stat = global_stat[0]
            stat['Durchschnittsgehalt_Gesamt'] = round(stat['Durchschnittsgehalt_Gesamt'], 2) if stat['Durchschnittsgehalt_Gesamt'] is not None else 'N/A'
            stat['Max_Gehalt_Gesamt'] = round(stat['Max_Gehalt_Gesamt'], 2) if stat['Max_Gehalt_Gesamt'] is not None else 'N/A'
            stat['Min_Gehalt_Gesamt'] = round(stat['Min_Gehalt_Gesamt'], 2) if stat['Min_Gehalt_Gesamt'] is not None else 'N/A'
            pprint.pprint(stat)
        else:
            print("Keine globalen Statistiken verfügbar (keine Mitarbeiter).")
    except PyMongoError as e:
        print(f"❌ Fehler beim Abrufen der globalen Gehaltsstatistiken: {e}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {e}")

# --- Erweiterte Suche ---

def suchen_mitarbeiter() -> None:
    """Erweiterte Mitarbeitersuche nach Name, Vorname, Stelle oder Abteilungsname."""
    print("\n--- ERWEITERTE SUCHE ---")

    try:
        suchbegriff = input("Geben Sie einen Suchbegriff ein (Name, Vorname, Stelle oder Abteilungsname): ").strip()
        if not suchbegriff:
            print("Suchbegriff darf nicht leer sein.")
            return

        # Pipeline pour die Suche, die auch Abteilungsnamen berücksichtigt
        pipeline = [
            {
                '$lookup': {
                    'from': 'abteilung',
                    'localField': 'abteilung_id',
                    'foreignField': '_id',
                    'as': 'abteilung_info'
                }
            },
            {
                '$unwind': {
                    'path': '$abteilung_info',
                    'preserveNullAndEmptyArrays': True
                }
            },
            {
                '$match': {
                    '$or': [
                        {'name': {'$regex': suchbegriff, '$options': 'i'}},
                        {'vorname': {'$regex': suchbegriff, '$options': 'i'}},
                        {'stelle': {'$regex': suchbegriff, '$options': 'i'}},
                        {'abteilung_info.abteilungsname': {'$regex': suchbegriff, '$options': 'i'}}
                    ]
                }
            },
            {
                '$project': {
                    '_id': 1,
                    'name': 1,
                    'vorname': 1,
                    'geburtsdatum': 1,
                    'stelle': 1,
                    'gehalt': 1,
                    'einstellungsdatum': 1,
                    'aktualisierung': 1,
                    'abteilung_id': 1,
                    'abteilung_name': {'$ifNull': ['$abteilung_info.abteilungsname', 'Unbekannt']}
                }
            }
        ]

        ergebnisse = list(mitarbeiter_collection.aggregate(pipeline))

        if not ergebnisse:
            print("❌ Keine Ergebnisse gefunden.")
            return

        print(f"\n{len(ergebnisse)} Ergebnis(se) gefunden:")
        for emp in ergebnisse:
            anzeigen_mitarbeiter(emp) # Nutzt die angepasste Anzeige pour aggregierte Daten

    except PyMongoError as e:
        print(f"❌ Fehler bei der Suche: {e}")
    except Exception as e:
        print(f"❌ Ein unerwarteter Fehler ist aufgetreten: {str(e)}")

# --- Hauptmenü ---

def hauptmenue() -> None:
    """Zeigt das Hauptmenü an und verwaltet die Benutzerwahl."""
    while True:
        print("\n=== MITARBEITER- & PROJEKTVERWALTUNG ===")
        print("1. Mitarbeiter hinzufügen")
        print("2. Mitarbeiter auflisten")
        print("3. Mitarbeiter aktualisieren")
        print("4. Mitarbeiter löschen")
        print("5. Abteilung hinzufügen")
        print("6. Abteilungsmanager zuweisen (Transaktion)")
        print("7. Projekt hinzufügen")
        print("8. Projekte auflisten")
        print("9. Mitarbeiter Projekt zuweisen")
        print("10. Statistiken")
        print("11. Erweiterte Suche")
        print("12. Beenden")

        wahl = input("\nIhre Wahl (1-12): ").strip()

        if wahl == "1":
            hinzufuegen_mitarbeiter()
        elif wahl == "2":
            auflisten_mitarbeiter()
        elif wahl == "3":
            aktualisieren_mitarbeiter()
        elif wahl == "4":
            loeschen_mitarbeiter()
        elif wahl == "5":
            hinzufuegen_abteilung()
        elif wahl == "6":
            setze_abteilungsmanager()
        elif wahl == "7":
            hinzufuegen_projekt()
        elif wahl == "8":
            auflisten_projekte()
        elif wahl == "9":
            zuweisen_mitarbeiter_projekt()
        elif wahl == "10":
            aggregationsstatistiken()
        elif wahl == "11":
            suchen_mitarbeiter()
        elif wahl == "12":
            print("Auf Wiedersehen!")
            client.close()
            break
        else:
            print("Ungültige Wahl. Bitte geben Sie eine Zahl zwischen 1 und 12 ein.")

if __name__ == "__main__":
    hauptmenue()
