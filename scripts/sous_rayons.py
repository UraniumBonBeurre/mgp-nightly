"""
scripts/sous_rayons.py — Sous-rayons de chaque rayon (21/11) : source unique, recopiée dans la table sous_rayons
================================================================================================================
(rayon, slug) → noms en français, anglais, allemand, italien, espagnol. Classement des catégories de boutique par
Gemini : scripts/classify_categories.py. Modifier ici puis lancer `python3 scripts/sous_rayons.py --sync`.
"""

TAXONOMY = {
    "tv-video": [
        ("televiseurs", "Téléviseurs", "TVs", "Fernseher", "Televisori", "Televisores"),
        ("videoprojecteurs", "Vidéoprojecteurs", "Projectors", "Beamer", "Videoproiettori", "Proyectores"),
        ("lecteurs-box", "Lecteurs et box TV", "Players and TV boxes", "Player und TV-Boxen", "Lettori e TV box", "Reproductores y TV box"),
        ("supports-accessoires-tv", "Supports et accessoires TV", "TV mounts and accessories", "TV-Halterungen und Zubehör", "Supporti e accessori TV", "Soportes y accesorios de TV"),
    ],
    "ordinateurs": [
        ("pc-portables", "PC portables", "Laptops", "Laptops", "Portatili", "Portátiles"),
        ("pc-bureau", "PC de bureau", "Desktop PCs", "Desktop-PCs", "PC desktop", "Ordenadores de sobremesa"),
        ("tablettes", "Tablettes et liseuses", "Tablets and e-readers", "Tablets und E-Reader", "Tablet ed e-reader", "Tabletas y e-readers"),
        ("accessoires-ordinateurs", "Sacoches et accessoires", "Bags and accessories", "Taschen und Zubehör", "Borse e accessori", "Fundas y accesorios"),
    ],
    "composants-pc": [
        ("cartes-graphiques", "Cartes graphiques", "Graphics cards", "Grafikkarten", "Schede video", "Tarjetas gráficas"),
        ("processeurs", "Processeurs", "Processors", "Prozessoren", "Processori", "Procesadores"),
        ("cartes-meres", "Cartes mères", "Motherboards", "Mainboards", "Schede madri", "Placas base"),
        ("memoire", "Mémoire RAM", "Memory (RAM)", "Arbeitsspeicher", "Memoria RAM", "Memoria RAM"),
        ("stockage-interne", "SSD et disques internes", "SSDs and internal drives", "SSDs und interne Festplatten", "SSD e dischi interni", "SSD y discos internos"),
        ("alimentations-boitiers", "Alimentations et boîtiers", "Power supplies and cases", "Netzteile und Gehäuse", "Alimentatori e case", "Fuentes y cajas"),
        ("refroidissement", "Refroidissement", "Cooling", "Kühlung", "Raffreddamento", "Refrigeración"),
    ],
    "ecrans": [
        ("ecrans-gaming", "Écrans gaming", "Gaming monitors", "Gaming-Monitore", "Monitor gaming", "Monitores gaming"),
        ("ecrans-bureautique", "Écrans bureautique et pro", "Office and pro monitors", "Büro- und Profi-Monitore", "Monitor ufficio e pro", "Monitores de oficina y pro"),
        ("accessoires-ecrans", "Supports et accessoires", "Stands and accessories", "Halterungen und Zubehör", "Supporti e accessori", "Soportes y accesorios"),
    ],
    "peripheriques": [
        ("claviers", "Claviers", "Keyboards", "Tastaturen", "Tastiere", "Teclados"),
        ("souris", "Souris et tapis", "Mice and mouse pads", "Mäuse und Mauspads", "Mouse e tappetini", "Ratones y alfombrillas"),
        ("webcams-micros", "Webcams et micros", "Webcams and microphones", "Webcams und Mikrofone", "Webcam e microfoni", "Webcams y micrófonos"),
        ("cables-hubs", "Câbles, hubs et adaptateurs", "Cables, hubs and adapters", "Kabel, Hubs und Adapter", "Cavi, hub e adattatori", "Cables, hubs y adaptadores"),
        ("autres-peripheriques", "Autres périphériques", "Other peripherals", "Weiteres Zubehör", "Altre periferiche", "Otros periféricos"),
    ],
    "impression": [
        ("imprimantes", "Imprimantes et scanners", "Printers and scanners", "Drucker und Scanner", "Stampanti e scanner", "Impresoras y escáneres"),
        ("encre-toner", "Encre et toner", "Ink and toner", "Tinte und Toner", "Inchiostro e toner", "Tinta y tóner"),
        ("papier-etiquettes", "Papier et étiquettes", "Paper and labels", "Papier und Etiketten", "Carta ed etichette", "Papel y etiquetas"),
        ("impression-3d", "Impression 3D", "3D printing", "3D-Druck", "Stampa 3D", "Impresión 3D"),
    ],
    "reseau-stockage": [
        ("wifi-routeurs", "Wi-Fi et routeurs", "Wi-Fi and routers", "WLAN und Router", "Wi-Fi e router", "Wi-Fi y routers"),
        ("switchs-cables-reseau", "Switchs et câbles réseau", "Switches and network cables", "Switches und Netzwerkkabel", "Switch e cavi di rete", "Switches y cables de red"),
        ("nas-serveurs", "NAS et serveurs", "NAS and servers", "NAS und Server", "NAS e server", "NAS y servidores"),
        ("disques-externes", "Disques externes", "External drives", "Externe Festplatten", "Dischi esterni", "Discos externos"),
        ("cles-cartes-memoire", "Clés USB et cartes mémoire", "USB drives and memory cards", "USB-Sticks und Speicherkarten", "Chiavette USB e schede di memoria", "Memorias USB y tarjetas"),
    ],
    "smartphones": [
        ("smartphones", "Smartphones", "Smartphones", "Smartphones", "Smartphone", "Smartphones"),
        ("coques-protections", "Coques et protections", "Cases and screen protectors", "Hüllen und Schutzfolien", "Cover e protezioni", "Fundas y protectores"),
        ("chargeurs-batteries", "Chargeurs, câbles et batteries", "Chargers, cables and power banks", "Ladegeräte, Kabel und Powerbanks", "Caricatori, cavi e power bank", "Cargadores, cables y baterías"),
        ("telephones-fixes", "Téléphones fixes et seniors", "Home and senior phones", "Festnetz- und Seniorentelefone", "Telefoni fissi e per anziani", "Teléfonos fijos y para mayores"),
        ("autres-accessoires-mobiles", "Autres accessoires", "Other accessories", "Weiteres Zubehör", "Altri accessori", "Otros accesorios"),
    ],
    "audio": [
        ("casques-ecouteurs", "Casques et écouteurs", "Headphones and earbuds", "Kopfhörer und Ohrhörer", "Cuffie e auricolari", "Auriculares"),
        ("enceintes", "Enceintes", "Speakers", "Lautsprecher", "Casse", "Altavoces"),
        ("barres-de-son-home-cinema", "Barres de son et home cinéma", "Soundbars and home cinema", "Soundbars und Heimkino", "Soundbar e home cinema", "Barras de sonido y cine en casa"),
        ("hifi-platines", "Hi-Fi, platines et radios", "Hi-fi, turntables and radios", "HiFi, Plattenspieler und Radios", "Hi-fi, giradischi e radio", "Hi-fi, tocadiscos y radios"),
        ("micros-audio-pro", "Micros et audio pro", "Microphones and pro audio", "Mikrofone und Profi-Audio", "Microfoni e audio pro", "Micrófonos y audio profesional"),
    ],
    "photo": [
        ("appareils-photo", "Appareils photo", "Cameras", "Kameras", "Fotocamere", "Cámaras"),
        ("objectifs", "Objectifs", "Lenses", "Objektive", "Obiettivi", "Objetivos"),
        ("drones-cameras-sport", "Drones et caméras sport", "Drones and action cameras", "Drohnen und Actioncams", "Droni e action cam", "Drones y cámaras deportivas"),
        ("accessoires-photo", "Accessoires photo", "Camera accessories", "Kamerazubehör", "Accessori foto", "Accesorios de fotografía"),
    ],
    "gaming": [
        ("consoles", "Consoles", "Consoles", "Konsolen", "Console", "Consolas"),
        ("jeux-video", "Jeux vidéo", "Video games", "Videospiele", "Videogiochi", "Videojuegos"),
        ("manettes-accessoires-gaming", "Manettes et accessoires", "Controllers and accessories", "Controller und Zubehör", "Controller e accessori", "Mandos y accesorios"),
        ("sieges-bureaux-gaming", "Sièges et bureaux gaming", "Gaming chairs and desks", "Gaming-Stühle und -Tische", "Sedie e scrivanie gaming", "Sillas y escritorios gaming"),
    ],
    "objets-connectes": [
        ("montres-connectees", "Montres et bracelets connectés", "Smartwatches and fitness trackers", "Smartwatches und Fitnesstracker", "Smartwatch e fitness tracker", "Relojes y pulseras inteligentes"),
        ("maison-connectee", "Maison connectée", "Smart home", "Smart Home", "Casa intelligente", "Hogar inteligente"),
        ("securite-cameras", "Caméras et sécurité", "Security cameras", "Sicherheitskameras", "Videocamere di sicurezza", "Cámaras de seguridad"),
        ("trackers-gadgets", "Traceurs et gadgets", "Trackers and gadgets", "Tracker und Gadgets", "Localizzatori e gadget", "Localizadores y gadgets"),
    ],
    "gros-electromenager": [
        ("lavage-sechage", "Lave-linge et sèche-linge", "Washers and dryers", "Waschmaschinen und Trockner", "Lavatrici e asciugatrici", "Lavadoras y secadoras"),
        ("lave-vaisselle", "Lave-vaisselle", "Dishwashers", "Geschirrspüler", "Lavastoviglie", "Lavavajillas"),
        ("froid", "Réfrigérateurs et congélateurs", "Fridges and freezers", "Kühl- und Gefrierschränke", "Frigoriferi e congelatori", "Frigoríficos y congeladores"),
        ("cuisson", "Fours, plaques et hottes", "Ovens, hobs and hoods", "Backöfen, Kochfelder und Hauben", "Forni, piani cottura e cappe", "Hornos, placas y campanas"),
    ],
    "petit-electromenager": [
        ("cafe-the", "Café et thé", "Coffee and tea", "Kaffee und Tee", "Caffè e tè", "Café y té"),
        ("preparation-culinaire", "Préparation culinaire", "Food preparation", "Küchenmaschinen", "Preparazione alimenti", "Preparación de alimentos"),
        ("cuisson-friteuses", "Cuisson et friteuses", "Cooking and air fryers", "Kochen und Heißluftfritteusen", "Cottura e friggitrici", "Cocina y freidoras"),
        ("aspirateurs-nettoyage", "Aspirateurs et nettoyage", "Vacuums and cleaning", "Staubsauger und Reinigung", "Aspirapolvere e pulizia", "Aspiradoras y limpieza"),
        ("soin-linge", "Fers et soin du linge", "Irons and garment care", "Bügeleisen und Wäschepflege", "Ferri e cura dei capi", "Planchas y cuidado de la ropa"),
        ("climatisation-chauffage", "Climatisation et chauffage", "Air conditioning and heating", "Klima und Heizung", "Climatizzazione e riscaldamento", "Climatización y calefacción"),
        ("soin-personnel", "Soin personnel électrique", "Personal care appliances", "Körperpflegegeräte", "Cura della persona", "Cuidado personal"),
    ],
    "beaute-sante": [
        ("parfums", "Parfums", "Fragrances", "Düfte", "Profumi", "Perfumes"),
        ("maquillage", "Maquillage", "Make-up", "Make-up", "Trucco", "Maquillaje"),
        ("soins-visage-corps", "Soins du visage et du corps", "Face and body care", "Gesichts- und Körperpflege", "Cura di viso e corpo", "Cuidado facial y corporal"),
        ("cheveux", "Cheveux", "Hair care", "Haarpflege", "Capelli", "Cabello"),
        ("sante-pharmacie", "Santé et pharmacie", "Health and pharmacy", "Gesundheit und Apotheke", "Salute e farmacia", "Salud y farmacia"),
        ("complements-nutrition", "Compléments et nutrition", "Supplements and nutrition", "Nahrungsergänzung", "Integratori e nutrizione", "Suplementos y nutrición"),
        ("hygiene-bien-etre", "Hygiène et bien-être", "Hygiene and wellbeing", "Hygiene und Wohlbefinden", "Igiene e benessere", "Higiene y bienestar"),
    ],
    "maison": [
        ("decoration", "Décoration", "Home decor", "Dekoration", "Decorazione", "Decoración"),
        ("linge-de-maison", "Linge de maison et literie", "Bedding and home textiles", "Bettwäsche und Heimtextilien", "Biancheria e tessili", "Ropa de cama y textiles"),
        ("cuisine-art-de-la-table", "Cuisine et art de la table", "Kitchen and tableware", "Küche und Tischkultur", "Cucina e tavola", "Cocina y menaje"),
        ("meubles-rangement", "Meubles et rangement", "Furniture and storage", "Möbel und Aufbewahrung", "Mobili e contenitori", "Muebles y almacenaje"),
        ("luminaires", "Luminaires", "Lighting", "Leuchten", "Illuminazione", "Iluminación"),
        ("bebe-puericulture", "Bébé et puériculture", "Baby and nursery", "Baby und Kinderzimmer", "Neonati e prima infanzia", "Bebé y puericultura"),
    ],
    "jouets": [
        ("jouets-enfants", "Jouets", "Toys", "Spielzeug", "Giocattoli", "Juguetes"),
        ("jeux-de-societe-puzzles", "Jeux de société et puzzles", "Board games and puzzles", "Gesellschaftsspiele und Puzzles", "Giochi da tavolo e puzzle", "Juegos de mesa y puzles"),
        ("construction-maquettes", "Construction et maquettes", "Building sets and models", "Bausätze und Modellbau", "Costruzioni e modellismo", "Construcción y maquetas"),
        ("loisirs-creatifs", "Loisirs créatifs", "Arts and crafts", "Basteln und Kreatives", "Attività creative", "Manualidades"),
        ("jeux-plein-air", "Jeux de plein air", "Outdoor play", "Outdoor-Spielzeug", "Giochi all'aperto", "Juegos al aire libre"),
    ],
    "sport": [
        ("velos-trottinettes", "Vélos et trottinettes", "Bikes and scooters", "Fahrräder und Roller", "Bici e monopattini", "Bicicletas y patinetes"),
        ("fitness-musculation", "Fitness et musculation", "Fitness and strength", "Fitness und Krafttraining", "Fitness e pesi", "Fitness y musculación"),
        ("glisse-sports-hiver", "Glisse et sports d'hiver", "Skate, ski and board sports", "Skate-, Ski- und Boardsport", "Skate, sci e tavola", "Patinaje, esquí y tabla"),
        ("randonnee-camping", "Randonnée et camping", "Hiking and camping", "Wandern und Camping", "Escursionismo e campeggio", "Senderismo y camping"),
        ("sports-collectifs-raquettes", "Sports collectifs et raquettes", "Team and racket sports", "Mannschafts- und Schlägersport", "Sport di squadra e racchetta", "Deportes de equipo y raqueta"),
        ("vetements-chaussures-sport", "Vêtements et chaussures de sport", "Sportswear and shoes", "Sportbekleidung und -schuhe", "Abbigliamento e scarpe sportive", "Ropa y calzado deportivo"),
        ("nautisme-peche", "Nautisme et pêche", "Water sports and fishing", "Wassersport und Angeln", "Sport acquatici e pesca", "Deportes acuáticos y pesca"),
    ],
    "bricolage-jardin": [
        ("outillage", "Outillage", "Tools", "Werkzeug", "Utensili", "Herramientas"),
        ("jardin-exterieur", "Jardin et extérieur", "Garden and outdoor", "Garten und Außenbereich", "Giardino ed esterni", "Jardín y exterior"),
        ("electricite-eclairage", "Électricité", "Electrical", "Elektro", "Elettricità", "Electricidad"),
        ("quincaillerie-plomberie", "Quincaillerie et plomberie", "Hardware and plumbing", "Eisenwaren und Sanitär", "Ferramenta e idraulica", "Ferretería y fontanería"),
        ("peinture-materiaux", "Peinture et matériaux", "Paint and materials", "Farbe und Baumaterial", "Pittura e materiali", "Pintura y materiales"),
        ("piscine-spa", "Piscine et spa", "Pools and spas", "Pool und Spa", "Piscine e spa", "Piscinas y spas"),
    ],
    "bureau-papeterie": [
        ("fournitures", "Fournitures de bureau", "Office supplies", "Bürobedarf", "Forniture per ufficio", "Material de oficina"),
        ("ecriture-papeterie", "Écriture et papeterie", "Writing and stationery", "Schreibwaren", "Scrittura e cancelleria", "Escritura y papelería"),
        ("mobilier-bureau", "Mobilier de bureau", "Office furniture", "Büromöbel", "Mobili per ufficio", "Mobiliario de oficina"),
        ("classement-emballage", "Classement et emballage", "Filing and packaging", "Ablage und Verpackung", "Archiviazione e imballaggio", "Archivo y embalaje"),
    ],
    "logiciels": [
        ("logiciels", "Logiciels", "Software", "Software", "Software", "Software"),
        ("cartes-cadeaux-abonnements", "Cartes cadeaux et abonnements", "Gift cards and subscriptions", "Geschenkkarten und Abos", "Carte regalo e abbonamenti", "Tarjetas regalo y suscripciones"),
    ],
    "mode": [
        ("bijoux", "Bijoux", "Jewelry", "Schmuck", "Gioielli", "Joyas"),
        ("montres", "Montres", "Watches", "Uhren", "Orologi", "Relojes"),
        ("chaussures", "Chaussures", "Shoes", "Schuhe", "Scarpe", "Calzado"),
        ("vetements-femme", "Vêtements femme", "Women's clothing", "Damenmode", "Abbigliamento donna", "Ropa de mujer"),
        ("vetements-homme", "Vêtements homme", "Men's clothing", "Herrenmode", "Abbigliamento uomo", "Ropa de hombre"),
        ("vetements-enfant", "Vêtements enfant", "Kids' clothing", "Kindermode", "Abbigliamento bambino", "Ropa infantil"),
        ("vetements", "Vêtements (mixte)", "Clothing (unisex)", "Bekleidung (Unisex)", "Abbigliamento (unisex)", "Ropa (unisex)"),
        ("sacs-maroquinerie", "Sacs et maroquinerie", "Bags and leather goods", "Taschen und Lederwaren", "Borse e pelletteria", "Bolsos y marroquinería"),
        ("lunettes-accessoires", "Lunettes et accessoires", "Sunglasses and accessories", "Brillen und Accessoires", "Occhiali e accessori", "Gafas y accesorios"),
        ("perles-creation-bijoux", "Perles et création de bijoux", "Beads and jewelry making", "Perlen und Schmuckherstellung", "Perline e creazione gioielli", "Abalorios y bisutería"),
    ],
    "animaux": [
        ("chiens", "Chiens", "Dogs", "Hunde", "Cani", "Perros"),
        ("chats", "Chats", "Cats", "Katzen", "Gatti", "Gatos"),
        ("autres-animaux", "Autres animaux", "Other pets", "Andere Haustiere", "Altri animali", "Otros animales"),
    ],
}


def codes() -> list[str]:
    """« rayon/sous-rayon » de tous les sous-rayons."""
    return [f"{r}/{s[0]}" for r, subs in TAXONOMY.items() for s in subs]


def sync():
    import json, sys, urllib.request
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    import settings
    rows = [{"rayon": r, "slug": s[0], "position": i, "label_fr": s[1], "label_en": s[2], "label_de": s[3],
             "label_it": s[4], "label_es": s[5]} for r, subs in TAXONOMY.items() for i, s in enumerate(subs)]
    req = urllib.request.Request(f"{settings.SUPABASE_URL}/rest/v1/sous_rayons?on_conflict=rayon,slug", method="POST",
                                 data=json.dumps(rows).encode(),
                                 headers={"apikey": settings.SUPABASE_KEY, "Authorization": f"Bearer {settings.SUPABASE_KEY}",
                                          "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates,return=minimal"})
    urllib.request.urlopen(req, timeout=60).read()
    print(f"{len(rows)} sous-rayons enregistrés")


if __name__ == "__main__":
    import sys
    if "--sync" in sys.argv:
        sync()
    else:
        print(len(codes()), "sous-rayons")
