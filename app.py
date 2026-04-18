import hashlib
import html
import os
import secrets
import sqlite3
from datetime import datetime
from http import cookies
from urllib.parse import parse_qs, urlencode
from wsgiref.simple_server import make_server
from flask import Flask, Response, request


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "commerce.db")
STATIC_DIR = os.path.join(BASE_DIR, "static")
UPLOADS_DIR = os.path.join(STATIC_DIR, "uploads", "verification")
PRODUCT_UPLOADS_DIR = os.path.join(STATIC_DIR, "uploads", "products")
SESSION_COOKIE = "budhub_session"
APP_NAME = "Budhub"
APP_TAGLINE = "Licensed cannabis delivery with live menus, simple checkout, and clear order tracking."
CLEANUP_DONE = False

MENU_SECTIONS = ["Flower", "Edibles", "Concentrates", "General"]
STORE_CATEGORY_OPTIONS = ["All"] + MENU_SECTIONS
STRAIN_FILTER_OPTIONS = ["All", "Sativa", "Indica", "Hybrid", "Unspecified"]
MENU_SECTION_NOTES = {
    "Edibles": "Flavor options are listed in the product details when available.",
    "Concentrates": "Concentrate options are listed by jar size.",
    "Flower": "Flower includes the Double Stuffed 7G lineup and any other whole flower options.",
}
LAUNCH_MENU = [
    {
        "name": "THC Syrup 1000MG",
        "category": "Edibles",
        "description": "1000MG syrup. Flavors: Cherry, Grape, Strawberry Kiwi.",
        "price": 25.50,
        "stock": 15,
    },
    {
        "name": "1G Diamonds",
        "category": "Concentrates",
        "description": "1 gram concentrate jar.",
        "price": 25.50,
        "stock": 12,
    },
    {
        "name": "3G Diamonds",
        "category": "Concentrates",
        "description": "3 gram concentrate jar.",
        "price": 70.50,
        "stock": 10,
    },
    {
        "name": "Blue Dream OZ",
        "category": "Flower",
        "description": "Full ounce flower option.",
        "price": 100.50,
        "stock": 8,
    },
    {
        "name": "Blue Dream DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $15.50.",
        "price": 15.50,
        "stock": 14,
    },
    {
        "name": "Electric Lemon DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $15.50.",
        "price": 15.50,
        "stock": 10,
    },
    {
        "name": "Gelato DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $15.50.",
        "price": 15.50,
        "stock": 10,
    },
    {
        "name": "Pink Mimosas Smalls DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $17.50.",
        "price": 17.50,
        "stock": 10,
    },
    {
        "name": "Cotton Candy Smalls DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $17.50.",
        "price": 17.50,
        "stock": 10,
    },
    {
        "name": "LA Confidential DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
        "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-5.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/la-confidential",
    },
    {
        "name": "Wedding Cake DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Sour Candy DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
        "image_url": "https://leafly-public.imgix.net/strains/photos/5SPDG4T4TcSO8PgLgWHO_SourDiesel_AdobeStock_171888473.jpg?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/sour-diesel",
    },
    {
        "name": "Mimosa DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Biscotti DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Ice Cream Cake DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Lemon Cherry Gelato DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Jack Herer DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Frozen Runtz DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $25.50.",
        "price": 25.50,
        "stock": 10,
    },
    {
        "name": "Afghan Kush DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $30.50. Low stock batch.",
        "price": 30.50,
        "stock": 4,
        "image_url": "https://images.leafly.com/flower-images/defaults/long-fluffy-wispy/strain-7.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/afghan-kush",
    },
    {
        "name": "Sundae Driver DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $30.50.",
        "price": 30.50,
        "stock": 8,
        "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-17.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/sundae-driver",
    },
    {
        "name": "Northern Lights DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $30.50.",
        "price": 30.50,
        "stock": 8,
    },
    {
        "name": "Animal Mints DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $30.50.",
        "price": 30.50,
        "stock": 8,
        "image_url": "https://leafly-public.imgix.net/strains/photos/IaYQshrPTxiD2BOWHO1n_AnimalMints.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/animal-mints",
    },
    {
        "name": "White Widow DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $30.50.",
        "price": 30.50,
        "stock": 8,
    },
    {
        "name": "Garlic Breath DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
    },
    {
        "name": "Mimosa DS 7G Reserve",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
    },
    {
        "name": "Pineapple Express DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
        "image_url": "https://images.leafly.com/flower-images/pineapple-express.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/pineapple-express",
    },
    {
        "name": "OG Kush DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
    },
    {
        "name": "Purple Haze DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
        "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-10.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/purple-haze",
    },
    {
        "name": "Durban Poison DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50. Low stock batch.",
        "price": 35.50,
        "stock": 4,
    },
    {
        "name": "Biscotti DS 7G Reserve",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
    },
    {
        "name": "Gorilla Glue #4 DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $35.50.",
        "price": 35.50,
        "stock": 8,
    },
    {
        "name": "Gelatti DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $40.50.",
        "price": 40.50,
        "stock": 6,
    },
    {
        "name": "Wedding Cake DS 7G Reserve",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $40.50.",
        "price": 40.50,
        "stock": 6,
    },
    {
        "name": "GMO Cookies DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $40.50.",
        "price": 40.50,
        "stock": 6,
    },
    {
        "name": "Ice Cream Cake DS 7G Reserve",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $40.50.",
        "price": 40.50,
        "stock": 6,
    },
    {
        "name": "Super Sour Diesel DS 7G",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $50.50.",
        "price": 50.50,
        "stock": 5,
        "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-22.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill",
        "source_url": "https://www.leafly.com/strains/super-sour-diesel",
    },
    {
        "name": "OG Kush DS 7G Reserve",
        "category": "Flower",
        "description": "Double Stuffed 7G flower. Tier priced at $50.50.",
        "price": 50.50,
        "stock": 5,
    },
]

ROLE_LABELS = {
    "helpdesk": "Budhub Helpdesk",
    "admin": "Admin",
    "banker": "In-House Bank",
    "dispatcher": "Dispatch Lead",
    "picker": "Inventory Picker",
    "driver": "Driver",
    "client": "Customer",
}

STATUS_LABELS = {
    "PACKING": "Ready for Packing",
    "REVIEW_REQUIRED": "Needs Dispatcher Review",
    "READY_FOR_DISPATCH": "Ready for Driver Assignment",
    "READY_FOR_PICKUP": "Ready for Pickup",
    "DRIVER_ASSIGNED": "Driver Assigned",
    "OUT_FOR_DELIVERY": "Out for Delivery",
    "DELIVERED": "Delivered",
    "CANCELED": "Canceled",
}

TRACKER = ["PACKING", "READY_FOR_DISPATCH", "OUT_FOR_DELIVERY", "DELIVERED"]
BLOCK_SIZE = 5
LEAFLY_STRAIN_LIBRARY = [
    {"name": "Blue Dream", "slug": "blue-dream", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/green/strain-3.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Biscotti", "slug": "biscotti", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/orange/strain-2.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Afghan Kush", "slug": "afghan-kush", "type": "Indica", "image_url": "https://images.leafly.com/flower-images/defaults/long-fluffy-wispy/strain-7.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Animal Mints", "slug": "animal-mints", "type": "Hybrid", "image_url": "https://leafly-public.imgix.net/strains/photos/IaYQshrPTxiD2BOWHO1n_AnimalMints.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Blue Nerds", "slug": "blue-nerds", "type": "Hybrid"},
    {"name": "Cotton Candy", "slug": "cotton-candy", "type": "Hybrid"},
    {"name": "Durban Poison", "slug": "durban-poison", "type": "Sativa", "image_url": "https://images.leafly.com/flower-images/defaults/light-green/strain-9.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Electric Lemon", "slug": "electric-lemon-g", "type": "Sativa"},
    {"name": "Frozen Runtz", "slug": "frozen-runtz", "type": "Hybrid"},
    {"name": "Garlic Breath", "slug": "garlic-breath", "type": "Hybrid"},
    {"name": "Gelato", "slug": "gelato", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-13.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Gelatti", "slug": "gelatti", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-4.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "GMO Cookies", "slug": "gmo-cookies", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-15.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Gorilla Glue #4", "slug": "original-glue", "type": "Hybrid"},
    {"name": "Ice Cream Cake", "slug": "ice-cream-cake", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-12.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Jack Herer", "slug": "jack-herer", "type": "Sativa", "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-8.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "LA Confidential", "slug": "la-confidential", "type": "Indica", "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-5.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Lemon Cherry Gelato", "slug": "lemon-cherry-gelato", "type": "Hybrid"},
    {"name": "Maui Gushers", "slug": "maui-gushers", "type": "Hybrid"},
    {"name": "Mimosa", "slug": "mimosa", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/orange/strain-16.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Northern Lights", "slug": "northern-lights", "type": "Indica", "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-6.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Obama Runtz", "slug": "obama-runtz", "type": "Hybrid"},
    {"name": "OG Kush", "slug": "og-kush", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/light-green/strain-11.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Pineapple Express", "slug": "pineapple-express", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/pineapple-express.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Pink Mimosas", "slug": "pink-mimosa", "type": "Hybrid"},
    {"name": "Purple Haze", "slug": "purple-haze", "type": "Sativa", "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-10.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Sour Candy", "slug": "sour-diesel", "type": "Hybrid", "image_url": "https://leafly-public.imgix.net/strains/photos/5SPDG4T4TcSO8PgLgWHO_SourDiesel_AdobeStock_171888473.jpg?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Strawberry Gumbo", "slug": "strains/strawberry-gum", "type": "Hybrid"},
    {"name": "Sundae Driver", "slug": "sundae-driver", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/purple/strain-17.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Super Sour Diesel", "slug": "super-sour-diesel", "type": "Sativa", "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-22.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "Wedding Cake", "slug": "wedding-cake", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/generic/strain-21.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
    {"name": "White Widow", "slug": "white-widow", "type": "Hybrid", "image_url": "https://images.leafly.com/flower-images/defaults/white/strain-14.png?auto=compress&w=1200&h=630&fit=crop&bg=FFFFFF&fit=fill"},
]


def db_connection():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def now_iso():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def hash_password(password):
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def format_money(value):
    return f"${value:,.2f}"


def is_double_stuffed_product(product):
    name = str(product["name"]).upper()
    description = str(product["description"]).upper()
    return "DS 7G" in name or "DOUBLE STUFFED" in description


def normalize_store_category(value):
    candidate = (value or "All").strip().title()
    return candidate if candidate in STORE_CATEGORY_OPTIONS else "All"


def normalize_strain_type(value):
    candidate = (value or "All").strip().title()
    return candidate if candidate in STRAIN_FILTER_OPTIONS else "All"


def infer_product_metadata(name, category, description=""):
    text = f"{name} {description}".lower()
    if category == "Flower":
        menu_group = "Flower"
    elif category == "Concentrates":
        menu_group = "Concentrates"
    elif category == "Edibles":
        menu_group = "Edibles"
    else:
        menu_group = "General"

    if "diamonds" in text:
        menu_group = "Diamonds"
    elif "syrup" in text:
        menu_group = "Syrup"
    elif "ounce" in text or " oz" in text:
        menu_group = "Full Ounce"
    elif is_double_stuffed_product({"name": name, "description": description}):
        menu_group = "Double Stuffed 7G"

    if category not in {"Flower", "Concentrates"}:
        return menu_group, ""

    if "sativa" in text:
        return menu_group, "Sativa"
    if "indica" in text:
        return menu_group, "Indica"
    if "hybrid" in text:
        return menu_group, "Hybrid"

    sativa_terms = ("electric lemon", "purple haze")
    indica_terms = ("afghan kush", "la confidential")
    hybrid_terms = (
        "animal mints",
        "sundae driver",
        "gorilla glue",
        "lemon cherry gelato",
        "frozen runtz",
        "pineapple express",
        "mendo berries",
        "cherry crushers",
        "pink mimosas",
        "cotton candy",
        "cinnamon roll runtz",
        "maui gushers",
        "garlic breath",
        "frozen pink runtz",
        "obama runtz",
        "newyork gumbo",
        "sour candy",
        "strawberry gumbo",
        "blue nerds",
        "sweet exotic candy",
    )
    if any(term in text for term in sativa_terms):
        return menu_group, "Sativa"
    if any(term in text for term in indica_terms):
        return menu_group, "Indica"
    if any(term in text for term in hybrid_terms):
        return menu_group, "Hybrid"
    return menu_group, "Unspecified"


def normalized_store_filters(raw_filters=None):
    filters = raw_filters or {}
    return {
        "category": normalize_store_category(filters.get("category")),
        "strain": normalize_strain_type(filters.get("strain")),
        "search": (filters.get("search", "") or "").strip(),
    }


def store_query(filters=None, **overrides):
    merged = normalized_store_filters(filters)
    for key, value in overrides.items():
        if key == "category":
            merged["category"] = normalize_store_category(value)
        elif key == "strain":
            merged["strain"] = normalize_strain_type(value)
        elif key == "search":
            merged["search"] = (value or "").strip()
    query = {}
    if merged["category"] != "All":
        query["category"] = merged["category"]
    if merged["strain"] != "All":
        query["strain"] = merged["strain"]
    if merged["search"]:
        query["search"] = merged["search"]
    return urlencode(query)


def store_url(filters=None, **overrides):
    query = store_query(filters, **overrides)
    return f"/?{query}" if query else "/"


def redirect_with_message(start_response, location, message, cookie_header=None):
    base, hash_fragment = location.split("#", 1) if "#" in location else (location, "")
    separator = "&" if "?" in base else "?"
    target = f"{base}{separator}{urlencode({'message': message})}"
    if hash_fragment:
        target = f"{target}#{hash_fragment}"
    return redirect(start_response, target, cookie_header=cookie_header)


def active_store_note(category):
    if category == "All":
        return "Use the submenu, search, and strain filters to narrow the live menu without leaving the page."
    return MENU_SECTION_NOTES.get(category, "Live inventory available now.")


def render_store_chip(label, url, active=False):
    class_name = "filter-chip active" if active else "filter-chip"
    return f'<a class="{class_name}" href="{html.escape(url)}">{html.escape(label)}</a>'


def render_store_search(filters):
    return f"""
    <form method="get" action="/" class="store-search">
      <input type="hidden" name="category" value="{html.escape(filters['category'])}">
      <input type="hidden" name="strain" value="{html.escape(filters['strain'])}">
      <label class="search-label">
        <span class="eyebrow">Search by Name</span>
        <input type="search" name="search" value="{html.escape(filters['search'])}" placeholder="Search flower, concentrates, syrup...">
      </label>
      <div class="card-buttons">
        <button type="submit">Search</button>
        <a class="button ghost" href="{store_url()}">Clear</a>
      </div>
    </form>
    """


def render_cart_widget(connection, user, filters):
    if not user or user["role"] != "client":
        return """
        <aside class="panel cart-widget" id="bag-widget">
          <span class="eyebrow">Bag Widget</span>
          <h2>Sign in to build a bag</h2>
          <p>Customers can add items, keep browsing the menu, and check out from this same page.</p>
          <a class="button" href="/login">Customer Login</a>
        </aside>
        """

    items = cart_items_for_user(connection, user["id"])
    subtotal = sum(item["quantity"] * item["product_price"] for item in items)
    return_to = store_url(filters) + "#bag-widget"
    rows = []
    for item in items:
        rows.append(
            f"""
            <div class="bag-item">
              <div>
                <strong>{html.escape(item["product_name"])}</strong>
                <p>{item["quantity"]} x {format_money(item["product_price"])}</p>
              </div>
              <div class="bag-item-actions">
                <span>{format_money(item["quantity"] * item["product_price"])}</span>
                <form method="post" action="/cart/remove">
                  <input type="hidden" name="product_id" value="{item["product_id"]}">
                  <input type="hidden" name="return_to" value="{html.escape(return_to)}">
                  <button class="button ghost" type="submit">Remove</button>
                </form>
              </div>
            </div>
            """
        )
    return f"""
    <aside class="panel cart-widget" id="bag-widget">
      <div class="bag-head">
        <div>
          <span class="eyebrow">Bag Widget</span>
          <h2>Your Bag</h2>
        </div>
        <span class="menu-count">{client_cart_count(connection, user["id"])} items</span>
      </div>
      <div class="bag-list">{''.join(rows) if rows else '<p>Your bag is empty. Add something and keep browsing.</p>'}</div>
      <div class="checkout-total"><span>Subtotal</span><strong>{format_money(subtotal)}</strong></div>
      <div class="checkout-total"><span>Available Credits</span><strong>{format_money(user["credit_balance"])}</strong></div>
      <div class="tracker-note">Checkout stays on the storefront now, so customers can review the bag while browsing.</div>
      <form method="post" action="/cart/checkout" class="form-grid bag-checkout">
        <input type="hidden" name="return_to" value="{html.escape(return_to)}">
        <label>How will you get it?
          <select name="fulfillment_type">
            <option value="DELIVERY">Delivery</option>
            <option value="PICKUP">Pick Up In Person</option>
          </select>
        </label>
        <label>Delivery Address or Pickup Note<textarea name="shipping_address" placeholder="Required for delivery, optional for pickup"></textarea></label>
        <label>Coupon Code<input type="text" name="coupon_code" placeholder="Optional"></label>
        <label class="checkbox-row"><input type="checkbox" name="use_credits" value="yes"> Apply available account credits ({format_money(user["credit_balance"])})</label>
        <label>Driver Note<textarea name="customer_note" placeholder="Gate code, apartment, or delivery note"></textarea></label>
        <button type="submit" {'disabled' if not items else ''}>Place One Grouped Order</button>
      </form>
    </aside>
    """


def slugify_name(value):
    cleaned = []
    for char in value.lower():
        if char.isalnum():
            cleaned.append(char)
        elif char in {" ", "-", "#"}:
            cleaned.append("-")
    slug = "".join(cleaned)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-")


def canonical_leafly_url(slug):
    if not slug:
        return ""
    if slug.startswith("http://") or slug.startswith("https://"):
        return slug
    if slug.startswith("strains/") or slug.startswith("brands/"):
        return f"https://www.leafly.com/{slug}"
    return f"https://www.leafly.com/strains/{slug}"


def seed_leafly_strains(connection):
    for strain in LEAFLY_STRAIN_LIBRARY:
        connection.execute(
            """
            INSERT INTO leafly_strains (name, slug, source_url, strain_type, image_url, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
                slug = excluded.slug,
                source_url = excluded.source_url,
                strain_type = excluded.strain_type,
                image_url = COALESCE(excluded.image_url, leafly_strains.image_url)
            """,
            (
                strain["name"],
                strain["slug"],
                canonical_leafly_url(strain["slug"]),
                strain["type"],
                strain.get("image_url"),
                now_iso(),
            ),
        )


def leafly_strain_rows(connection):
    return connection.execute("SELECT * FROM leafly_strains ORDER BY name COLLATE NOCASE ASC").fetchall()


def infer_leafly_reference(connection, product_name):
    cleaned = (
        product_name.replace(" DS 7G", "")
        .replace(" Smalls", "")
        .replace(" Reserve", "")
        .replace(" OZ", "")
        .strip()
    )
    direct = connection.execute(
        "SELECT * FROM leafly_strains WHERE lower(name) = lower(?)",
        (cleaned,),
    ).fetchone()
    if direct:
        return direct
    for row in leafly_strain_rows(connection):
        if row["name"].lower() in cleaned.lower() or cleaned.lower() in row["name"].lower():
            return row
    return None


def parse_cookies(environ):
    jar = cookies.SimpleCookie()
    if environ.get("HTTP_COOKIE"):
        jar.load(environ["HTTP_COOKIE"])
    return jar


def query_params(environ):
    raw = environ.get("QUERY_STRING", "")
    parsed = parse_qs(raw)
    return {key: value[0] for key, value in parsed.items()}


def read_post_data(environ):
    try:
        size = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        size = 0
    raw = environ["wsgi.input"].read(size).decode("utf-8")
    parsed = parse_qs(raw)
    return {key: value[0].strip() for key, value in parsed.items()}


def read_multipart_form(environ):
    content_type = environ.get("CONTENT_TYPE", "")
    boundary_key = "boundary="
    if boundary_key not in content_type:
        return {}, {}
    boundary = content_type.split(boundary_key, 1)[1].strip().strip('"')
    try:
        size = int(environ.get("CONTENT_LENGTH") or "0")
    except ValueError:
        size = 0
    raw = environ["wsgi.input"].read(size)
    delimiter = ("--" + boundary).encode()
    data = {}
    files = {}
    for part in raw.split(delimiter):
        part = part.strip()
        if not part or part == b"--":
            continue
        headers_raw, _, body = part.partition(b"\r\n\r\n")
        if not body:
            continue
        body = body[:-2] if body.endswith(b"\r\n") else body
        headers = headers_raw.decode("utf-8", errors="ignore").split("\r\n")
        header_map = {}
        for header in headers:
            if ":" in header:
                key, value = header.split(":", 1)
                header_map[key.lower().strip()] = value.strip()
        disposition = header_map.get("content-disposition", "")
        attributes = {}
        for chunk in disposition.split(";"):
            if "=" in chunk:
                key, value = chunk.split("=", 1)
                attributes[key.strip()] = value.strip().strip('"')
        field_name = attributes.get("name")
        if not field_name:
            continue
        filename = attributes.get("filename")
        if filename:
            files[field_name] = {
                "filename": filename,
                "content": body,
                "type": header_map.get("content-type", ""),
            }
        else:
            data[field_name] = body.decode("utf-8", errors="ignore").strip()
    return data, files


def redirect(start_response, location, cookie_header=None):
    headers = [("Location", location)]
    if cookie_header:
        headers.append(("Set-Cookie", cookie_header))
    start_response("302 Found", headers)
    return [b""]


def text_response(start_response, body, status="200 OK", content_type="text/html; charset=utf-8"):
    start_response(status, [("Content-Type", content_type)])
    return [body.encode("utf-8")]


def table_exists(connection, name):
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return bool(row)


def column_exists(connection, table_name, column_name):
    columns = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(column["name"] == column_name for column in columns)


def ensure_column(connection, table_name, definition):
    column_name = definition.split()[0]
    if not column_exists(connection, table_name, column_name):
        connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {definition}")


def sync_launch_menu(connection):
    launch_names = {item["name"] for item in LAUNCH_MENU}
    for item in LAUNCH_MENU:
        leafly_reference = infer_leafly_reference(connection, item["name"])
        menu_group, strain_type = infer_product_metadata(item["name"], item["category"], item["description"])
        if leafly_reference and item["category"] in {"Flower", "Concentrates"}:
            item.setdefault("source_url", leafly_reference["source_url"])
            item.setdefault("image_url", leafly_reference["image_url"])
            if strain_type == "Unspecified":
                strain_type = normalize_strain_type(leafly_reference["strain_type"])
        existing = connection.execute("SELECT id FROM products WHERE name = ?", (item["name"],)).fetchone()
        if existing:
            connection.execute(
                """
                UPDATE products
                SET category = ?, description = ?, image_url = ?, source_url = ?, leafly_strain_name = ?, price = ?, stock = ?, menu_group = ?, strain_type = ?
                WHERE id = ?
                """,
                (
                    item["category"],
                    item["description"],
                    item.get("image_url"),
                    item.get("source_url"),
                    leafly_reference["name"] if leafly_reference else None,
                    item["price"],
                    item["stock"],
                    menu_group,
                    strain_type,
                    existing["id"],
                ),
            )
        else:
            connection.execute(
                """
                INSERT INTO products (name, category, description, image_url, source_url, leafly_strain_name, price, stock, menu_group, strain_type, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["name"],
                    item["category"],
                    item["description"],
                    item.get("image_url"),
                    item.get("source_url"),
                    leafly_reference["name"] if leafly_reference else None,
                    item["price"],
                    item["stock"],
                    menu_group,
                    strain_type,
                    now_iso(),
                ),
            )

    existing_products = connection.execute("SELECT id, name FROM products").fetchall()
    for product in existing_products:
        if product["name"] in launch_names:
            continue
        referenced = connection.execute(
            "SELECT 1 FROM ticket_items WHERE product_id = ? LIMIT 1",
            (product["id"],),
        ).fetchone()
        if referenced:
            connection.execute(
                "UPDATE products SET stock = 0, category = 'General' WHERE id = ?",
                (product["id"],),
            )
        else:
            connection.execute("DELETE FROM cart_items WHERE product_id = ?", (product["id"],))
            connection.execute("DELETE FROM products WHERE id = ?", (product["id"],))


def get_current_user(environ, connection):
    jar = parse_cookies(environ)
    session_cookie = jar.get(SESSION_COOKIE)
    if not session_cookie:
        return None
    return connection.execute(
        """
        SELECT users.*
        FROM sessions
        JOIN users ON users.id = sessions.user_id
        WHERE sessions.token = ?
        """,
        (session_cookie.value,),
    ).fetchone()


def create_session(connection, user_id):
    token = secrets.token_hex(24)
    connection.execute(
        "INSERT INTO sessions (token, user_id, created_at) VALUES (?, ?, ?)",
        (token, user_id, now_iso()),
    )
    connection.commit()
    return token


def destroy_session(environ, connection):
    jar = parse_cookies(environ)
    session_cookie = jar.get(SESSION_COOKIE)
    if session_cookie:
        connection.execute("DELETE FROM sessions WHERE token = ?", (session_cookie.value,))
        connection.commit()


def init_db():
    global CLEANUP_DONE
    os.makedirs(STATIC_DIR, exist_ok=True)
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    with db_connection() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL,
                account_state TEXT NOT NULL DEFAULT 'ACTIVE',
                account_reason TEXT,
                credit_balance REAL NOT NULL DEFAULT 0,
                verification_status TEXT NOT NULL DEFAULT 'VERIFIED',
                verification_note TEXT,
                id_front_path TEXT,
                id_back_path TEXT,
                id_selfie_path TEXT,
                verified_at TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'General',
                description TEXT NOT NULL,
                image_url TEXT,
                source_url TEXT,
                leafly_strain_name TEXT,
                price REAL NOT NULL,
                stock INTEGER NOT NULL,
                menu_group TEXT NOT NULL DEFAULT '',
                strain_type TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS leafly_strains (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                slug TEXT NOT NULL,
                source_url TEXT NOT NULL,
                strain_type TEXT NOT NULL DEFAULT 'Unspecified',
                image_url TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS cart_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, product_id),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_number TEXT NOT NULL UNIQUE,
                client_id INTEGER NOT NULL,
                fulfillment_type TEXT NOT NULL DEFAULT 'DELIVERY',
                shipping_address TEXT NOT NULL,
                customer_note TEXT,
                status TEXT NOT NULL,
                payment_status TEXT NOT NULL,
                coupon_code TEXT,
                discount_amount REAL NOT NULL DEFAULT 0,
                credit_applied REAL NOT NULL DEFAULT 0,
                banker_id INTEGER,
                dispatcher_id INTEGER,
                picker_id INTEGER,
                driver_id INTEGER,
                delivery_block_id INTEGER,
                review_reason TEXT,
                cancel_reason TEXT,
                internal_note TEXT,
                stock_released INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (client_id) REFERENCES users(id),
                FOREIGN KEY (banker_id) REFERENCES users(id),
                FOREIGN KEY (dispatcher_id) REFERENCES users(id),
                FOREIGN KEY (picker_id) REFERENCES users(id),
                FOREIGN KEY (driver_id) REFERENCES users(id),
                FOREIGN KEY (delivery_block_id) REFERENCES delivery_blocks(id)
            );

            CREATE TABLE IF NOT EXISTS delivery_blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_name TEXT NOT NULL UNIQUE,
                dispatcher_id INTEGER NOT NULL,
                driver_id INTEGER,
                status TEXT NOT NULL DEFAULT 'OPEN',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                submitted_at TEXT,
                FOREIGN KEY (dispatcher_id) REFERENCES users(id),
                FOREIGN KEY (driver_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS ticket_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                product_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL,
                locked_price REAL NOT NULL,
                FOREIGN KEY (ticket_id) REFERENCES tickets(id) ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES products(id)
            );

            CREATE TABLE IF NOT EXISTS support_tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                opened_by INTEGER NOT NULL,
                category TEXT NOT NULL,
                subject TEXT,
                priority TEXT NOT NULL DEFAULT 'NORMAL',
                related_ticket_id INTEGER,
                reason TEXT NOT NULL,
                status TEXT NOT NULL,
                resolution_note TEXT,
                assigned_to INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (opened_by) REFERENCES users(id),
                FOREIGN KEY (related_ticket_id) REFERENCES tickets(id),
                FOREIGN KEY (assigned_to) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                support_ticket_id INTEGER NOT NULL,
                author_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (support_ticket_id) REFERENCES support_tickets(id) ON DELETE CASCADE,
                FOREIGN KEY (author_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS activity_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_id INTEGER,
                actor_role TEXT,
                target_user_id INTEGER,
                action TEXT NOT NULL,
                details TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (actor_id) REFERENCES users(id),
                FOREIGN KEY (target_user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS guest_help_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                issue TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                response_note TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS coupons (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                discount_type TEXT NOT NULL,
                discount_value REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS credit_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                issued_by INTEGER NOT NULL,
                amount REAL NOT NULL,
                note TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (issued_by) REFERENCES users(id)
            );
            """
        )
        ensure_column(connection, "users", "account_state TEXT NOT NULL DEFAULT 'ACTIVE'")
        ensure_column(connection, "users", "account_reason TEXT")
        ensure_column(connection, "users", "credit_balance REAL NOT NULL DEFAULT 0")
        ensure_column(connection, "users", "verification_status TEXT NOT NULL DEFAULT 'VERIFIED'")
        ensure_column(connection, "users", "verification_note TEXT")
        ensure_column(connection, "users", "id_front_path TEXT")
        ensure_column(connection, "users", "id_back_path TEXT")
        ensure_column(connection, "users", "id_selfie_path TEXT")
        ensure_column(connection, "users", "verified_at TEXT")
        ensure_column(connection, "support_tickets", "priority TEXT NOT NULL DEFAULT 'NORMAL'")
        ensure_column(connection, "support_tickets", "related_ticket_id INTEGER")
        ensure_column(connection, "support_tickets", "subject TEXT")
        ensure_column(connection, "support_tickets", "assigned_to INTEGER")
        ensure_column(connection, "products", "category TEXT NOT NULL DEFAULT 'General'")
        ensure_column(connection, "products", "image_url TEXT")
        ensure_column(connection, "products", "source_url TEXT")
        ensure_column(connection, "products", "leafly_strain_name TEXT")
        ensure_column(connection, "products", "menu_group TEXT NOT NULL DEFAULT ''")
        ensure_column(connection, "products", "strain_type TEXT NOT NULL DEFAULT ''")
        ensure_column(connection, "tickets", "fulfillment_type TEXT NOT NULL DEFAULT 'DELIVERY'")
        ensure_column(connection, "tickets", "coupon_code TEXT")
        ensure_column(connection, "tickets", "discount_amount REAL NOT NULL DEFAULT 0")
        ensure_column(connection, "tickets", "credit_applied REAL NOT NULL DEFAULT 0")
        ensure_column(connection, "tickets", "delivery_block_id INTEGER")

        seed_leafly_strains(connection)
        seed_defaults(connection)
        if not CLEANUP_DONE:
            cleanup_generated_tickets(connection)
            CLEANUP_DONE = True
        connection.commit()


def seed_defaults(connection):
    users = [
        ("Budhub Helpdesk", "helpdesk@ecommerce.local", "helpdesk123", "helpdesk"),
        ("System Admin", "admin@ecommerce.local", "admin123", "admin"),
        ("Budhub Bank", "bank@ecommerce.local", "bank123", "banker"),
        ("Dispatch Lead", "dispatcher@ecommerce.local", "dispatch123", "dispatcher"),
        ("Warehouse Picker", "picker@ecommerce.local", "picker123", "picker"),
        ("Delivery Driver", "driver@ecommerce.local", "driver123", "driver"),
        ("Demo Customer", "client@ecommerce.local", "client123", "client"),
    ]
    for name, email, password, role in users:
        existing = connection.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if existing:
            connection.execute(
                "UPDATE users SET verification_status = 'VERIFIED', account_state = CASE WHEN account_state = 'PENDING_VERIFICATION' THEN 'ACTIVE' ELSE account_state END WHERE id = ?",
                (existing["id"],),
            )
            continue
        connection.execute(
            """
            INSERT INTO users (
                name, email, password_hash, role, account_state, verification_status, verified_at, created_at
            ) VALUES (?, ?, ?, ?, 'ACTIVE', 'VERIFIED', ?, ?)
            """,
            (name, email, hash_password(password), role, now_iso(), now_iso()),
        )

    sync_launch_menu(connection)


def cleanup_generated_tickets(connection):
    demo_client = connection.execute(
        "SELECT id FROM users WHERE email = ?",
        ("client@ecommerce.local",),
    ).fetchone()
    if demo_client:
        connection.execute(
            "UPDATE users SET account_state = 'ACTIVE', account_reason = NULL, verification_status = 'VERIFIED', verification_note = NULL WHERE id = ?",
            (demo_client["id"],),
        )
        connection.execute("DELETE FROM support_tickets WHERE user_id = ?", (demo_client["id"],))
        connection.execute("DELETE FROM tickets WHERE client_id = ?", (demo_client["id"],))
    connection.execute("DELETE FROM tickets WHERE ticket_number LIKE 'BH-LEGACY-%'")


def flash_message(message, level="info"):
    if not message:
        return ""
    return f'<div class="flash flash-{html.escape(level)}">{html.escape(message)}</div>'


def render_help_button(user):
    if user:
        return '<a class="support-fab" href="/help">Budhub Help</a>'
    return '<a class="support-fab" href="/register#support-access">Need Help?</a>'


def render_nav(user, cart_count=0):
    links = ['<a href="/">Menu</a>']
    if user:
        links.append('<a href="/dashboard">Dashboard</a>')
        if user["role"] == "client":
            links.append(f'<a href="/#bag-widget">Bag ({cart_count})</a>')
        if user["role"] in {"admin", "helpdesk"}:
            links.append('<a href="/admin">Admin</a>')
        links.append(f'<span class="nav-user">{html.escape(user["name"])} ({html.escape(ROLE_LABELS.get(user["role"], user["role"]))})</span>')
        links.append('<a class="button ghost" href="/logout">Logout</a>')
    else:
        links.append('<a href="/login">Login</a>')
        links.append('<a class="button" href="/register">Create Account</a>')
    return "".join(links)


def page(title, body, user=None, message=None, level="info", cart_count=0):
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="stylesheet" href="/static/styles.css">
</head>
<body>
  <header class="site-header">
    <div class="brand">
      <span class="brand-kicker">{APP_NAME}</span>
      <h1>{APP_TAGLINE}</h1>
    </div>
    <nav>{render_nav(user, cart_count=cart_count)}</nav>
  </header>
  <main class="page-shell">
    {flash_message(message, level)}
    {body}
  </main>
  {render_help_button(user)}
</body>
</html>"""


def login_form(error=""):
    return f"""
    <section class="panel narrow">
      <h2>Login</h2>
      <p>Budhub sends each role to its own workspace.</p>
      {flash_message(error, "error")}
      <form method="post" action="/login" class="form-grid">
        <label>Email<input type="email" name="email" required></label>
        <label>Password<input type="password" name="password" required></label>
        <button type="submit">Sign In</button>
      </form>
    </section>
    """


def register_form(error=""):
    return f"""
    <section class="panel narrow">
      <h2>Create Customer Account</h2>
      <p>Upload ID front, ID back, and a selfie holding your ID. New accounts stay pending until admin verification is complete.</p>
      {flash_message(error, "error")}
      <form method="post" action="/register" class="form-grid" enctype="multipart/form-data">
        <label>Full Name<input type="text" name="name" required></label>
        <label>Email<input type="email" name="email" required></label>
        <label>Password<input type="password" name="password" minlength="6" required></label>
        <label>ID Front Photo<input type="file" name="id_front" accept="image/*" required></label>
        <label>ID Back Photo<input type="file" name="id_back" accept="image/*" required></label>
        <label>Selfie Holding ID<input type="file" name="id_selfie" accept="image/*" required></label>
        <button type="submit">Create Account</button>
      </form>
      <div class="demo-box" id="support-access">
        <strong>Need help with registration?</strong>
        <p class="demo-note">Use this form if you are blocked before you can finish account creation. After you have an account, the discreet Budhub Help button opens your full message thread.</p>
        <form method="post" action="/guest-help" class="form-grid">
          <label>Name<input type="text" name="name" required></label>
          <label>Email<input type="email" name="email" required></label>
          <label>Issue<textarea name="issue" required placeholder="Explain what is stopping you from creating or using your account"></textarea></label>
          <button type="submit" class="button ghost">Send Registration Help Request</button>
        </form>
      </div>
    </section>
    """


def status_badge(status):
    return f'<span class="badge badge-{html.escape(status.lower())}">{html.escape(STATUS_LABELS.get(status, status))}</span>'


def client_cart_count(connection, user_id):
    row = connection.execute("SELECT COALESCE(SUM(quantity), 0) AS count FROM cart_items WHERE user_id = ?", (user_id,)).fetchone()
    return row["count"] if row else 0


def cart_items_for_user(connection, user_id):
    return connection.execute(
        """
        SELECT cart_items.*, products.name AS product_name, products.description AS product_description,
               products.price AS product_price, products.stock AS product_stock
        FROM cart_items
        JOIN products ON products.id = cart_items.product_id
        WHERE cart_items.user_id = ?
        ORDER BY cart_items.created_at DESC
        """,
        (user_id,),
    ).fetchall()


def ticket_rows(connection, where_clause="", params=()):
    return connection.execute(
        f"""
        SELECT tickets.*,
               clients.name AS client_name,
               banker.name AS banker_name,
               dispatcher.name AS dispatcher_name,
               picker.name AS picker_name,
               driver.name AS driver_name,
               delivery_blocks.block_name AS delivery_block_name,
               delivery_blocks.status AS delivery_block_status,
               COALESCE(SUM(ticket_items.quantity * ticket_items.locked_price), 0) AS total_amount,
               COALESCE(SUM(ticket_items.quantity), 0) AS total_units,
               tickets.discount_amount AS discount_amount,
               tickets.credit_applied AS credit_applied
        FROM tickets
        JOIN users AS clients ON clients.id = tickets.client_id
        LEFT JOIN users AS banker ON banker.id = tickets.banker_id
        LEFT JOIN users AS dispatcher ON dispatcher.id = tickets.dispatcher_id
        LEFT JOIN users AS picker ON picker.id = tickets.picker_id
        LEFT JOIN users AS driver ON driver.id = tickets.driver_id
        LEFT JOIN delivery_blocks ON delivery_blocks.id = tickets.delivery_block_id
        LEFT JOIN ticket_items ON ticket_items.ticket_id = tickets.id
        {where_clause}
        GROUP BY tickets.id
        ORDER BY tickets.updated_at DESC, tickets.id DESC
        """,
        params,
    ).fetchall()


def ticket_items_map(connection, ticket_ids):
    if not ticket_ids:
        return {}
    placeholders = ",".join("?" for _ in ticket_ids)
    rows = connection.execute(
        f"""
        SELECT ticket_items.*, products.name AS product_name, products.stock AS product_stock
        FROM ticket_items
        JOIN products ON products.id = ticket_items.product_id
        WHERE ticket_items.ticket_id IN ({placeholders})
        ORDER BY ticket_items.id
        """,
        ticket_ids,
    ).fetchall()
    grouped = {ticket_id: [] for ticket_id in ticket_ids}
    for row in rows:
        grouped.setdefault(row["ticket_id"], []).append(row)
    return grouped


def single_ticket(connection, ticket_id):
    rows = ticket_rows(connection, "WHERE tickets.id = ?", (ticket_id,))
    return rows[0] if rows else None


def support_rows(connection, where_clause="", params=()):
    return connection.execute(
        f"""
        SELECT support_tickets.*,
               users.name AS user_name,
               users.email AS user_email,
               openers.name AS opened_by_name,
               assignees.name AS assigned_to_name,
               tickets.ticket_number AS related_ticket_number
        FROM support_tickets
        JOIN users ON users.id = support_tickets.user_id
        JOIN users AS openers ON openers.id = support_tickets.opened_by
        LEFT JOIN users AS assignees ON assignees.id = support_tickets.assigned_to
        LEFT JOIN tickets ON tickets.id = support_tickets.related_ticket_id
        {where_clause}
        ORDER BY support_tickets.updated_at DESC, support_tickets.id DESC
        """,
        params,
    ).fetchall()


def support_messages_map(connection, ticket_ids):
    if not ticket_ids:
        return {}
    placeholders = ",".join("?" for _ in ticket_ids)
    rows = connection.execute(
        f"""
        SELECT support_messages.*, users.name AS author_name, users.role AS author_role
        FROM support_messages
        JOIN users ON users.id = support_messages.author_id
        WHERE support_messages.support_ticket_id IN ({placeholders})
        ORDER BY support_messages.created_at ASC, support_messages.id ASC
        """,
        ticket_ids,
    ).fetchall()
    grouped = {ticket_id: [] for ticket_id in ticket_ids}
    for row in rows:
        grouped.setdefault(row["support_ticket_id"], []).append(row)
    return grouped


def activity_log_rows(connection, where_clause="", params=()):
    return connection.execute(
        f"""
        SELECT activity_logs.*,
               actors.name AS actor_name,
               targets.name AS target_user_name
        FROM activity_logs
        LEFT JOIN users AS actors ON actors.id = activity_logs.actor_id
        LEFT JOIN users AS targets ON targets.id = activity_logs.target_user_id
        {where_clause}
        ORDER BY activity_logs.created_at DESC, activity_logs.id DESC
        """,
        params,
    ).fetchall()


def guest_help_rows(connection):
    return connection.execute(
        "SELECT * FROM guest_help_requests ORDER BY updated_at DESC, id DESC"
    ).fetchall()


def log_activity(connection, actor, action, details="", target_user_id=None):
    connection.execute(
        """
        INSERT INTO activity_logs (actor_id, actor_role, target_user_id, action, details, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            actor["id"] if actor else None,
            actor["role"] if actor else "",
            target_user_id,
            action,
            details,
            now_iso(),
        ),
    )


def coupon_rows(connection, where_clause="", params=()):
    return connection.execute(
        f"""
        SELECT *
        FROM coupons
        {where_clause}
        ORDER BY active DESC, code ASC
        """,
        params,
    ).fetchall()


def normalize_coupon_code(code):
    return (code or "").strip().upper()


def subtotal_from_items(items):
    return sum(item["quantity"] * item["locked_price"] for item in items)


def coupon_discount_amount(coupon, subtotal):
    if not coupon or subtotal <= 0:
        return 0.0
    if coupon["discount_type"] == "PERCENT":
        return round(min(subtotal, subtotal * (coupon["discount_value"] / 100.0)), 2)
    return round(min(subtotal, coupon["discount_value"]), 2)


def payment_summary(subtotal, discount_amount, credit_applied):
    due = round(max(0.0, subtotal - discount_amount - credit_applied), 2)
    return {
        "subtotal": round(subtotal, 2),
        "discount_amount": round(discount_amount, 2),
        "credit_applied": round(credit_applied, 2),
        "payment_due": due,
    }


EMERGENCY_CONFIG = {
    "medical_emergency": {
        "label": "Medical Emergency",
        "priority": "HIGH",
        "ui_class": "emergency-medical",
        "driver_message": "Dispatch is notified. Proceed with your emergency and await for a message to your phone.",
    },
    "car_accident": {
        "label": "Car Accident",
        "priority": "CRITICAL",
        "ui_class": "emergency-accident",
        "driver_message": "Dial 911 now and begin the process of an accident report. Dispatch and admin have been alerted as a red priority emergency.",
    },
    "robbery": {
        "label": "Robbery",
        "priority": "CRITICAL",
        "ui_class": "emergency-robbery",
        "driver_message": "Just comply. Your life is not worth small amounts of anything. Return to base immediately when you are safe.",
    },
    "traffic_stop": {
        "label": "Traffic Stop",
        "priority": "MEDIUM",
        "ui_class": "emergency-traffic",
        "driver_message": "Do not panic. You are never traveling with an illegal amount of anything, so comply and you will be okay.",
    },
}


def emergency_meta(emergency_type):
    return EMERGENCY_CONFIG.get(
        emergency_type,
        {"label": "Emergency", "priority": "HIGH", "ui_class": "emergency-default", "driver_message": "Dispatch has been notified."},
    )


def save_verification_upload(user_id, label, file_info):
    filename = file_info.get("filename") or ""
    extension = os.path.splitext(filename)[1].lower()
    if extension not in {".jpg", ".jpeg", ".png", ".webp"}:
        extension = ".jpg"
    content = file_info.get("content") or b""
    if not content:
        raise ValueError("All verification images are required.")
    if len(content) > 8 * 1024 * 1024:
        raise ValueError("Each verification image must be under 8MB.")
    saved_name = f"user_{user_id}_{label}_{secrets.token_hex(6)}{extension}"
    absolute_path = os.path.join(UPLOADS_DIR, saved_name)
    with open(absolute_path, "wb") as handle:
        handle.write(content)
    return f"/static/uploads/verification/{saved_name}"


def restricted_account_page(user):
    reason = html.escape(user["account_reason"] or "This account is currently restricted.")
    if user["account_state"] == "PENDING_VERIFICATION":
        state = "Pending Verification"
        reason = html.escape(user["verification_note"] or "Your account is waiting for admin ID review.")
    else:
        state = html.escape(user["account_state"].title())
    body = f"""
    <section class="panel narrow">
      <h2>Account {state}</h2>
      <p>All account functions are disabled right now.</p>
      <div class="tracker-note warning-note">{reason}</div>
      <p>If this was a mistake, contact your Budhub support team.</p>
    </section>
    """
    return page("Account Restricted", body, user=user)


def account_restricted(user):
    return bool(user) and user["role"] not in {"admin", "helpdesk"} and user["account_state"] in {"LOCKED", "SUSPENDED", "BANNED", "PENDING_VERIFICATION"}


def render_credit_issue_panel(connection):
    users = connection.execute("SELECT id, name, email, credit_balance FROM users WHERE role = 'client' ORDER BY name").fetchall()
    options = "".join(
        f"<option value='{row['id']}'>{html.escape(row['name'])} ({html.escape(row['email'])}) - {format_money(row['credit_balance'])}</option>"
        for row in users
    )
    return f"""
    <section class="panel">
      <h2>Issue Credits</h2>
      <form method="post" action="/credits/issue" class="form-grid">
        <label>Customer<select name="user_id" required><option value="">Choose customer</option>{options}</select></label>
        <label>Amount<input type="number" name="amount" min="0.01" step="0.01" required></label>
        <label>Note<textarea name="note" required placeholder="Why are you adding credits?"></textarea></label>
        <button type="submit">Issue Credits</button>
      </form>
    </section>
    """


def tracker_index(status):
    if status == "REVIEW_REQUIRED":
        return 0
    if status == "DRIVER_ASSIGNED":
        return 1
    if status == "READY_FOR_PICKUP":
        return 1
    if status == "CANCELED":
        return -1
    return TRACKER.index(status)


def render_tracker(status):
    if status == "CANCELED":
        return "<div class='tracker-note canceled-note'>This ticket was canceled.</div>"
    current = tracker_index(status)
    blocks = []
    for index, step in enumerate(TRACKER):
        classes = ["tracker-step"]
        if index < current:
            classes.append("done")
        elif index == current:
            classes.append("current")
        label = STATUS_LABELS[step] if step != "READY_FOR_DISPATCH" else "Dispatch + Driver"
        blocks.append(f"<div class=\"{' '.join(classes)}\"><span>{index + 1}</span><strong>{html.escape(label)}</strong></div>")
    extra = ""
    if status == "REVIEW_REQUIRED":
        extra = "<div class='tracker-note warning-note'>Picker sent this order back for dispatcher review.</div>"
    if status == "DRIVER_ASSIGNED":
        extra = "<div class='tracker-note'>Dispatch assigned a driver. Delivery can only close after payment is verified.</div>"
    if status == "READY_FOR_PICKUP":
        extra = "<div class='tracker-note'>This order is waiting for an in-person pickup handoff.</div>"
    return f"<div class='tracker'>{''.join(blocks)}</div>{extra}"


def render_item_list(items):
    return "<div class='item-pill-list'>" + "".join(
        f"<div class='item-pill'><strong>{html.escape(item['product_name'])}</strong><span>{item['quantity']} x {format_money(item['locked_price'])}</span></div>"
        for item in items
    ) + "</div>"


def generate_ticket_number(connection):
    ticket_number = f"BH-{datetime.utcnow().strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    while connection.execute("SELECT id FROM tickets WHERE ticket_number = ?", (ticket_number,)).fetchone():
        ticket_number = f"BH-{datetime.utcnow().strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    return ticket_number


def generate_block_name(connection):
    block_name = f"BLOCK-{datetime.utcnow().strftime('%Y%m%d')}-{secrets.token_hex(2).upper()}"
    while connection.execute("SELECT id FROM delivery_blocks WHERE block_name = ?", (block_name,)).fetchone():
        block_name = f"BLOCK-{datetime.utcnow().strftime('%Y%m%d')}-{secrets.token_hex(2).upper()}"
    return block_name


def create_delivery_block(connection, dispatcher_id):
    block_name = generate_block_name(connection)
    cursor = connection.execute(
        """
        INSERT INTO delivery_blocks (block_name, dispatcher_id, status, created_at, updated_at)
        VALUES (?, ?, 'OPEN', ?, ?)
        """,
        (block_name, dispatcher_id, now_iso(), now_iso()),
    )
    return cursor.lastrowid


def delivery_block_rows(connection, where_clause="", params=()):
    return connection.execute(
        f"""
        SELECT delivery_blocks.*,
               dispatcher.name AS dispatcher_name,
               driver.name AS driver_name,
               COUNT(tickets.id) AS ticket_count,
               COALESCE(SUM(CASE WHEN tickets.status NOT IN ('CANCELED', 'DELIVERED') THEN 1 ELSE 0 END), 0) AS active_ticket_count
        FROM delivery_blocks
        LEFT JOIN users AS dispatcher ON dispatcher.id = delivery_blocks.dispatcher_id
        LEFT JOIN users AS driver ON driver.id = delivery_blocks.driver_id
        LEFT JOIN tickets ON tickets.delivery_block_id = delivery_blocks.id
        {where_clause}
        GROUP BY delivery_blocks.id
        ORDER BY delivery_blocks.updated_at DESC, delivery_blocks.id DESC
        """,
        params,
    ).fetchall()


def delivery_block_tickets_map(connection, block_ids):
    if not block_ids:
        return {}
    placeholders = ",".join("?" for _ in block_ids)
    rows = ticket_rows(connection, f"WHERE tickets.delivery_block_id IN ({placeholders})", tuple(block_ids))
    grouped = {block_id: [] for block_id in block_ids}
    for row in rows:
        grouped.setdefault(row["delivery_block_id"], []).append(row)
    return grouped


def create_ticket(connection, client_id, items, shipping_address, customer_note, fulfillment_type, coupon_code="", use_credits=False):
    ticket_number = generate_ticket_number(connection)
    timestamp = now_iso()
    client = connection.execute("SELECT * FROM users WHERE id = ?", (client_id,)).fetchone()
    preview_items = []
    subtotal = 0.0
    for item in items:
        product = connection.execute("SELECT * FROM products WHERE id = ?", (item["product_id"],)).fetchone()
        if not product:
            raise ValueError("Menu item not found")
        if item["quantity"] < 1 or item["quantity"] > product["stock"]:
            raise ValueError(f"Not enough stock for {product['name']}")
        preview_items.append((product, item["quantity"]))
        subtotal += item["quantity"] * product["price"]

    coupon = None
    coupon_code = normalize_coupon_code(coupon_code)
    if coupon_code:
        coupon = connection.execute("SELECT * FROM coupons WHERE code = ? AND active = 1", (coupon_code,)).fetchone()
        if not coupon:
            raise ValueError("Coupon code is not valid")
    discount_amount = coupon_discount_amount(coupon, subtotal)
    available_credit = float(client["credit_balance"] or 0)
    credit_applied = round(min(max(0.0, available_credit), max(0.0, subtotal - discount_amount)), 2) if use_credits else 0.0
    payment_status = "VERIFIED" if subtotal - discount_amount - credit_applied <= 0 else "PENDING"

    connection.execute(
        """
        INSERT INTO tickets (
            ticket_number, client_id, fulfillment_type, shipping_address, customer_note, status, payment_status,
            coupon_code, discount_amount, credit_applied, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, 'PACKING', ?, ?, ?, ?, ?, ?)
        """,
        (
            ticket_number,
            client_id,
            fulfillment_type,
            shipping_address,
            customer_note,
            payment_status,
            coupon_code or None,
            discount_amount,
            credit_applied,
            timestamp,
            timestamp,
        ),
    )
    ticket_id = connection.execute("SELECT id FROM tickets WHERE ticket_number = ?", (ticket_number,)).fetchone()["id"]
    for product, quantity in preview_items:
        connection.execute(
            "INSERT INTO ticket_items (ticket_id, product_id, quantity, locked_price) VALUES (?, ?, ?, ?)",
            (ticket_id, product["id"], quantity, product["price"]),
        )
        connection.execute("UPDATE products SET stock = stock - ? WHERE id = ?", (quantity, product["id"]))
    if credit_applied > 0:
        connection.execute(
            "UPDATE users SET credit_balance = credit_balance - ? WHERE id = ?",
            (credit_applied, client_id),
        )
        connection.execute(
            """
            INSERT INTO credit_ledger (user_id, issued_by, amount, note, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (client_id, client_id, -credit_applied, f"Applied to ticket {ticket_number}", now_iso()),
        )
    return ticket_id


def release_ticket_stock(connection, ticket_id):
    ticket = connection.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket or ticket["stock_released"]:
        return
    items = ticket_items_map(connection, [ticket_id]).get(ticket_id, [])
    for item in items:
        connection.execute("UPDATE products SET stock = stock + ? WHERE id = ?", (item["quantity"], item["product_id"]))
    connection.execute("UPDATE tickets SET stock_released = 1, updated_at = ? WHERE id = ?", (now_iso(), ticket_id))


def reserve_ticket_stock(connection, ticket_id):
    ticket = connection.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket or not ticket["stock_released"]:
        return
    items = ticket_items_map(connection, [ticket_id]).get(ticket_id, [])
    for item in items:
        product = connection.execute("SELECT * FROM products WHERE id = ?", (item["product_id"],)).fetchone()
        if not product or product["stock"] < item["quantity"]:
            raise ValueError(f"Not enough stock for {item['product_name']}")
    for item in items:
        connection.execute("UPDATE products SET stock = stock - ? WHERE id = ?", (item["quantity"], item["product_id"]))
    connection.execute("UPDATE tickets SET stock_released = 0, updated_at = ? WHERE id = ?", (now_iso(), ticket_id))


def render_store_page(connection, user=None, message=None, level="info", filters=None):
    filters = normalized_store_filters(filters)
    products = connection.execute(
        """
        SELECT *
        FROM products
        WHERE stock > 0
        ORDER BY
            CASE category
                WHEN 'Flower' THEN 1
                WHEN 'Edibles' THEN 2
                WHEN 'Concentrates' THEN 3
                ELSE 5
            END,
            CASE
                WHEN category = 'Flower' AND (name LIKE '%DS 7G%' OR description LIKE '%Double Stuffed%') THEN 0
                WHEN category = 'Flower' THEN 1
                ELSE 0
            END,
            price ASC,
            name COLLATE NOCASE ASC
        """
    ).fetchall()
    cart_count = client_cart_count(connection, user["id"]) if user and user["role"] == "client" else 0
    visible_products = []
    for product in products:
        if filters["category"] != "All" and product["category"] != filters["category"]:
            continue
        if filters["category"] in {"Flower", "Concentrates"} and filters["strain"] != "All":
            if normalize_strain_type(product["strain_type"] or "Unspecified") != filters["strain"]:
                continue
        if filters["search"] and filters["search"].lower() not in str(product["name"]).lower():
            continue
        visible_products.append(product)

    cards = []
    for product in visible_products:
        action = "<a class='button' href='/login'>Login to Order</a>"
        if user and user["role"] == "client":
            action = f"""
            <form method="post" action="/cart/add" class="card-action-stack">
              <input type="hidden" name="product_id" value="{product['id']}">
              <input type="hidden" name="return_to" value="{html.escape(store_url(filters))}">
              <label class="compact-label">Qty<input type="number" name="quantity" min="1" max="{product['stock']}" value="1" required></label>
              <div class="card-buttons">
                <button type="submit">Add to Bag</button>
                <a class="button ghost" href="/order?product_id={product['id']}">Order Now</a>
              </div>
            </form>
            """
        elif user:
            action = "<a class='button ghost' href='/dashboard'>Open Dashboard</a>"
        card_label = product["category"]
        if product["category"] == "Flower" and is_double_stuffed_product(product):
            card_label = "Flower | Double Stuffed 7G"
        cards.append(
            f"""
            <article class="product-card">
              {f'<img class="product-card-image" src="{html.escape(product["image_url"])}" alt="{html.escape(product["name"])}">' if product["image_url"] else ""}
              <div class="product-card-top">
                <span class="eyebrow">{html.escape(card_label)} | In Stock: {product["stock"]}</span>
                <h3>{html.escape(product["name"])}</h3>
                <div class="product-meta-pills">
                  <span class="price-pill">{format_money(product["price"])}</span>
                  {f"<span class='strain-pill'>{html.escape(normalize_strain_type(product['strain_type'] or 'Unspecified'))}</span>" if product["category"] in {"Flower", "Concentrates"} else ""}
                  {f"<span class='strain-pill'>{html.escape(product['menu_group'])}</span>" if product["menu_group"] else ""}
                </div>
              </div>
              <p>{html.escape(product["description"])}</p>
              {f'<a class="source-link" href="{html.escape(product["source_url"])}" target="_blank" rel="noopener noreferrer">Leafly Reference</a>' if product["source_url"] else ""}
              <div class="product-card-bottom">{action}</div>
            </article>
            """
        )

    category_chips = "".join(
        render_store_chip(option, store_url(filters, category=option, strain="All"), active=filters["category"] == option)
        for option in STORE_CATEGORY_OPTIONS
    )
    strain_controls = ""
    if filters["category"] in {"Flower", "Concentrates"}:
        strain_controls = f"""
        <div class="filter-row">
          <span class="eyebrow">Strain Filter</span>
          <div class="filter-chip-row">
            {''.join(render_store_chip(option, store_url(filters, strain=option), active=filters["strain"] == option) for option in STRAIN_FILTER_OPTIONS)}
          </div>
        </div>
        """

    body = f"""
    <section class="hero">
      <div>
        <span class="eyebrow">Licensed Cannabis Delivery</span>
        <h2>Shop the Budhub live menu with flower, concentrates, edibles, delivery, and pickup.</h2>
        <p>The storefront now uses submenu browsing, quick search by product name, and an in-page bag widget so customers can keep shopping without being kicked to a separate cart screen.</p>
        {f"<div class='tracker-note'>Available credits: {format_money(user['credit_balance'])}</div>" if user and user['role'] == 'client' else ""}
        <div class="hero-actions">
          <a class="button" href="{'/#bag-widget' if user and user['role'] == 'client' else '/login'}">{'Open Bag Widget' if user and user['role'] == 'client' else 'Customer Login'}</a>
          <a class="button ghost" href="{'/dashboard' if user else '/register'}">{'View Dashboard' if user else 'Create Account'}</a>
        </div>
      </div>
      <div class="hero-side">
        <div class="hero-flow">
          <div>Browse the menu and build your bag</div>
          <div>Choose delivery or pickup details</div>
          <div>Confirm the order and dispatch</div>
          <div>Track progress while the order is on the way</div>
          <div>Complete handoff with payment and ID check</div>
        </div>
        <div class="hero-summary">
          <span class="eyebrow">Storefront</span>
          <strong>{len(products)} items available</strong>
          <span>{cart_count} items ready in bag</span>
          <span>{sum(1 for product in products if product["category"] == "Flower" and is_double_stuffed_product(product))} Double Stuffed flower options</span>
        </div>
      </div>
    </section>
    <section class="store-layout">
      <div class="store-main">
        <section class="menu-section">
          <div class="menu-section-head">
            <div>
              <span class="eyebrow">Browse Submenus</span>
              <h3>{html.escape(filters["category"] if filters["category"] != "All" else "All Menu Items")}</h3>
            </div>
            <span class="menu-count">{len(visible_products)} matches</span>
          </div>
          <p class="menu-note">{html.escape(active_store_note(filters["category"]))}</p>
          <div class="filter-row">
            <span class="eyebrow">Menu Categories</span>
            <div class="filter-chip-row">{category_chips}</div>
          </div>
          {strain_controls}
          {render_store_search(filters)}
          <div class="product-grid">{''.join(cards) if cards else "<p>No menu items match that search or filter yet.</p>"}</div>
        </section>
      </div>
      {render_cart_widget(connection, user, filters)}
    </section>
    """
    return page(APP_NAME, body, user=user, message=message, level=level, cart_count=cart_count)


def order_form(connection, product_id, user, error=""):
    product = connection.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if not product:
        return None
    return page(
        "Place Order",
        f"""
        <section class="panel narrow">
          <h2>Place Order</h2>
          <p><strong>{html.escape(product["name"])}</strong> for {format_money(product["price"])} each.</p>
          {flash_message(error, "error")}
          <form method="post" action="/orders/create" class="form-grid">
            <input type="hidden" name="product_id" value="{product["id"]}">
            <label>Quantity<input type="number" name="quantity" min="1" max="{product["stock"]}" value="1" required></label>
            <label>How will you get it?
              <select name="fulfillment_type">
                <option value="DELIVERY">Delivery</option>
                <option value="PICKUP">Pick Up In Person</option>
              </select>
            </label>
            <label>Delivery Address or Pickup Note<textarea name="shipping_address" placeholder="Required for delivery, optional for pickup"></textarea></label>
            <label>Coupon Code<input type="text" name="coupon_code" placeholder="Optional"></label>
            <label class="checkbox-row"><input type="checkbox" name="use_credits" value="yes"> Apply available account credits ({format_money(user["credit_balance"])})</label>
            <label>Driver Note<textarea name="customer_note" placeholder="Gate code, apartment, or quick note"></textarea></label>
            <button type="submit">Submit Order</button>
          </form>
        </section>
        """,
        user=user,
        cart_count=client_cart_count(connection, user["id"]),
    )


def render_client_dashboard(connection, user, message=None, level="info"):
    tickets = ticket_rows(connection, "WHERE tickets.client_id = ?", (user["id"],))
    items_map = ticket_items_map(connection, [ticket["id"] for ticket in tickets])
    cards = []
    for ticket in tickets:
        notes = ""
        if ticket["review_reason"]:
            notes += f"<div class='tracker-note warning-note'>Review reason: {html.escape(ticket['review_reason'])}</div>"
        if ticket["cancel_reason"]:
            notes += f"<div class='tracker-note canceled-note'>Canceled: {html.escape(ticket['cancel_reason'])}</div>"
        cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div>
                  <span class="eyebrow">Ticket {html.escape(ticket["ticket_number"])}</span>
                  <h3>{html.escape(ticket["client_name"])}</h3>
                </div>
                {status_badge(ticket["status"])}
              </div>
              <div class="order-meta">
                <span>Payment: {html.escape(ticket["payment_status"])}</span>
                <span>Type: {html.escape(ticket["fulfillment_type"].title())}</span>
                <span>Total: {format_money(ticket["total_amount"])}</span>
                <span>Due: {format_money(max(0, ticket["total_amount"] - ticket["discount_amount"] - ticket["credit_applied"]))}</span>
                <span>Address / Pickup: {html.escape(ticket["shipping_address"])}</span>
              </div>
              <div class="order-meta">
                <span>Coupon: {html.escape(ticket["coupon_code"] or "None")}</span>
                <span>Discount: {format_money(ticket["discount_amount"])}</span>
                <span>Credits Used: {format_money(ticket["credit_applied"])}</span>
              </div>
              {render_item_list(items_map.get(ticket["id"], []))}
              {render_tracker(ticket["status"])}
              {notes}
              <div class="ticket-actions">
                {
                    f'''
                    <form method="post" action="/orders/update" class="action-stack">
                      <input type="hidden" name="order_id" value="{ticket["id"]}">
                      <input type="hidden" name="action" value="client_cancel">
                      <label>Cancel Reason<textarea name="reason" required placeholder="Tell us why you need to cancel"></textarea></label>
                      <button type="submit" class="danger">Cancel Order</button>
                    </form>
                    '''
                    if ticket["status"] not in {"DELIVERED", "CANCELED", "OUT_FOR_DELIVERY"} else "<span class='subtle'>This order can no longer be canceled online.</span>"
                }
              </div>
            </article>
            """
        )
    latest = STATUS_LABELS.get(tickets[0]["status"], tickets[0]["status"]) if tickets else "No Orders"
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Total Tickets</span><strong>{len(tickets)}</strong></div>
      <div class="stat-card"><span>Latest Status</span><strong>{html.escape(latest)}</strong></div>
      <div class="stat-card"><span>Bag Items</span><strong>{client_cart_count(connection, user["id"])}</strong></div>
      <div class="stat-card"><span>Credits</span><strong>{format_money(user["credit_balance"])}</strong></div>
    </section>
    <section class="panel">
      <div class="panel-head">
        <h2>Your Budhub Orders</h2>
        <div class="panel-actions">
          <a class="button ghost" href="/cart">View Bag</a>
          <a class="button" href="/">Browse Menu</a>
        </div>
      </div>
      <div class="order-card-grid">{''.join(cards) if cards else '<p>No orders yet.</p>'}</div>
    </section>
    """
    return page("Customer Dashboard", body, user=user, message=message, level=level, cart_count=client_cart_count(connection, user["id"]))


def render_cart_page(connection, user, message=None, level="info"):
    items = cart_items_for_user(connection, user["id"])
    subtotal = 0
    rows = []
    for item in items:
        subtotal += item["quantity"] * item["product_price"]
        rows.append(
            f"""
            <div class="cart-row">
              <div>
                <span class="eyebrow">Menu Item</span>
                <h3>{html.escape(item["product_name"])}</h3>
                <p>{html.escape(item["product_description"])}</p>
              </div>
              <div class="cart-row-meta">
                <span>Qty {item["quantity"]}</span>
                <strong>{format_money(item["quantity"] * item["product_price"])}</strong>
                <form method="post" action="/cart/remove">
                  <input type="hidden" name="product_id" value="{item["product_id"]}">
                  <input type="hidden" name="return_to" value="/cart">
                  <button class="button ghost" type="submit">Remove</button>
                </form>
              </div>
            </div>
            """
        )
    body = f"""
    <section class="cart-layout">
      <section class="panel">
        <div class="panel-head">
          <h2>Your Bag</h2>
          <a class="button ghost" href="/#bag-widget">Back to Menu</a>
        </div>
        <div class="cart-list">{''.join(rows) if rows else '<p>Your bag is empty.</p>'}</div>
      </section>
      <section class="panel">
        <h2>Checkout</h2>
        <div class="checkout-total"><span>Items</span><strong>{client_cart_count(connection, user["id"])}</strong></div>
        <div class="checkout-total"><span>Subtotal</span><strong>{format_money(subtotal)}</strong></div>
        <div class="checkout-total"><span>Available Credits</span><strong>{format_money(user["credit_balance"])}</strong></div>
        <div class="tracker-note">Budhub checkout creates one grouped ticket for the full bag.</div>
        <form method="post" action="/cart/checkout" class="form-grid">
          <input type="hidden" name="return_to" value="/cart">
          <label>How will you get it?
            <select name="fulfillment_type">
              <option value="DELIVERY">Delivery</option>
              <option value="PICKUP">Pick Up In Person</option>
            </select>
          </label>
          <label>Delivery Address or Pickup Note<textarea name="shipping_address" placeholder="Required for delivery, optional for pickup"></textarea></label>
          <label>Coupon Code<input type="text" name="coupon_code" placeholder="Optional"></label>
          <label class="checkbox-row"><input type="checkbox" name="use_credits" value="yes"> Apply available account credits ({format_money(user["credit_balance"])})</label>
          <label>Driver Note<textarea name="customer_note" placeholder="Gate code, apartment, or delivery note"></textarea></label>
          <button type="submit" {'disabled' if not items else ''}>Place One Grouped Order</button>
        </form>
      </section>
    </section>
    """
    return page("Your Bag", body, user=user, message=message, level=level, cart_count=client_cart_count(connection, user["id"]))


def render_banker_dashboard(connection, user, message=None, level="info"):
    tickets = ticket_rows(
        connection,
        "WHERE tickets.payment_status = 'PENDING' AND tickets.status NOT IN ('CANCELED', 'DELIVERED') OR tickets.banker_id = ?",
        (user["id"],),
    )
    items_map = ticket_items_map(connection, [ticket["id"] for ticket in tickets])
    cards = []
    for ticket in tickets:
        action = "<span class='subtle'>Payment already verified.</span>"
        if ticket["payment_status"] == "PENDING" and ticket["status"] not in {"CANCELED", "DELIVERED"}:
            action = f"""
            <form method="post" action="/orders/update" class="action-stack">
              <input type="hidden" name="order_id" value="{ticket["id"]}">
              <input type="hidden" name="action" value="verify_payment">
              <button type="submit">Verify Payment</button>
            </form>
            """
        cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div>
                  <span class="eyebrow">Ticket {html.escape(ticket["ticket_number"])}</span>
                  <h3>{html.escape(ticket["client_name"])}</h3>
                </div>
                {status_badge(ticket["status"])}
              </div>
                <div class="order-meta">
                  <span>Total: {format_money(ticket["total_amount"])}</span>
                  <span>Type: {html.escape(ticket["fulfillment_type"].title())}</span>
                  <span>Address: {html.escape(ticket["shipping_address"])}</span>
                  <span>Payment: {html.escape(ticket["payment_status"])}</span>
                  <span>Due: {format_money(max(0, ticket["total_amount"] - ticket["discount_amount"] - ticket["credit_applied"]))}</span>
                </div>
              {render_item_list(items_map.get(ticket["id"], []))}
              <div class="ticket-actions">{action}</div>
            </article>
            """
        )
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Waiting for Verification</span><strong>{sum(1 for ticket in tickets if ticket['payment_status'] == 'PENDING' and ticket['status'] not in {'CANCELED', 'DELIVERED'})}</strong></div>
      <div class="stat-card"><span>Tickets on Desk</span><strong>{len(tickets)}</strong></div>
    </section>
    <section class="panel"><h2>In-House Bank</h2><div class="order-card-grid">{''.join(cards) if cards else '<p>No payment reviews waiting.</p>'}</div></section>
    {render_credit_issue_panel(connection)}
    """
    return page("Bank Dashboard", body, user=user, message=message, level=level)


def render_dispatcher_dashboard(connection, user, message=None, level="info"):
    tickets = ticket_rows(connection, "", ())
    emergency_alerts = support_rows(connection, "WHERE support_tickets.category LIKE 'EMERGENCY_%' AND support_tickets.status != 'CLOSED'", ())
    items_map = ticket_items_map(connection, [ticket["id"] for ticket in tickets])
    blocks = delivery_block_rows(connection)
    block_ticket_map = delivery_block_tickets_map(connection, [block["id"] for block in blocks])
    drivers = connection.execute("SELECT id, name FROM users WHERE role = 'driver' ORDER BY name").fetchall()
    driver_options = "".join(f"<option value='{driver['id']}'>{html.escape(driver['name'])}</option>" for driver in drivers)
    open_blocks = [block for block in blocks if block["status"] == "OPEN"]
    block_options = "".join(f"<option value='{block['id']}'>{html.escape(block['block_name'])} ({block['active_ticket_count']}/{BLOCK_SIZE})</option>" for block in open_blocks)
    cards = []
    for ticket in tickets:
        actions = []
        if ticket["status"] == "READY_FOR_PICKUP":
            actions.append(
                f"""
                <form method="post" action="/orders/update" class="action-stack">
                  <input type="hidden" name="order_id" value="{ticket["id"]}">
                  <input type="hidden" name="action" value="complete_pickup">
                  <button type="submit">Mark Picked Up</button>
                </form>
                """
            )
        if ticket["status"] == "READY_FOR_DISPATCH":
            actions.append(
                f"""
                <form method="post" action="/orders/update" class="action-stack">
                  <input type="hidden" name="order_id" value="{ticket["id"]}">
                  <input type="hidden" name="action" value="assign_to_block">
                  <label>Assign to Block
                    <select name="block_id" required>
                      <option value="">Choose open block</option>
                      {block_options}
                    </select>
                  </label>
                  <button type="submit" {'disabled' if not open_blocks else ''}>Assign Ticket to Block</button>
                </form>
                """
            )
            actions.append(
                f"""
                <form method="post" action="/orders/update" class="action-stack">
                  <input type="hidden" name="order_id" value="{ticket["id"]}">
                  <input type="hidden" name="action" value="create_block_for_ticket">
                  <button type="submit">Create New Block and Add Ticket</button>
                </form>
                """
            )
        if ticket["status"] == "REVIEW_REQUIRED":
            products = connection.execute("SELECT id, name, stock FROM products ORDER BY name").fetchall()
            selectors = []
            for item in items_map.get(ticket["id"], []):
                options = "".join(
                    f"<option value='{product['id']}' {'selected' if product['id'] == item['product_id'] else ''}>{html.escape(product['name'])} ({product['stock']} in stock)</option>"
                    for product in products
                )
                selectors.append(f"<label>{html.escape(item['product_name'])} x {item['quantity']}<select name='replacement_{item['id']}'>{options}</select></label>")
            actions.append(
                f"""
                <form method="post" action="/orders/update" class="action-stack">
                  <input type="hidden" name="order_id" value="{ticket["id"]}">
                  <input type="hidden" name="action" value="resolve_review">
                  <div class="reason-box">Picker review: {html.escape(ticket["review_reason"] or "Needs product change")}</div>
                  {''.join(selectors)}
                  <button type="submit">Switch Product and Return to Packing</button>
                </form>
                """
            )
        if ticket["status"] in {"DRIVER_ASSIGNED", "OUT_FOR_DELIVERY"}:
            actions.append(
                f"""
                <form method="post" action="/orders/update" class="action-stack">
                  <input type="hidden" name="order_id" value="{ticket["id"]}">
                  <input type="hidden" name="action" value="pull_back">
                  <label>Pull Back Reason<textarea name="reason" required placeholder="Why is this coming back to dispatch?"></textarea></label>
                  <button type="submit">Pull Back</button>
                </form>
                """
            )
        if ticket["status"] not in {"DELIVERED", "CANCELED"}:
            actions.append(
                f"""
                <form method="post" action="/orders/update" class="action-stack">
                  <input type="hidden" name="order_id" value="{ticket["id"]}">
                  <input type="hidden" name="action" value="cancel_order">
                  <label>Cancel Reason<textarea name="reason" required placeholder="Why is this ticket canceled?"></textarea></label>
                  <button type="submit" class="danger">Cancel Ticket</button>
                </form>
                """
            )
        cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div><span class="eyebrow">Ticket {html.escape(ticket["ticket_number"])}</span><h3>{html.escape(ticket["client_name"])}</h3></div>
                {status_badge(ticket["status"])}
              </div>
                <div class="order-meta">
                  <span>Total: {format_money(ticket["total_amount"])}</span>
                  <span>Type: {html.escape(ticket["fulfillment_type"].title())}</span>
                  <span>Driver: {html.escape(ticket["driver_name"] or 'Unassigned')}</span>
                  <span>Block: {html.escape(ticket["delivery_block_name"] or 'Not in block')}</span>
                  <span>Payment: {html.escape(ticket["payment_status"])}</span>
                  <span>Due: {format_money(max(0, ticket["total_amount"] - ticket["discount_amount"] - ticket["credit_applied"]))}</span>
                </div>
              {render_item_list(items_map.get(ticket["id"], []))}
              {render_tracker(ticket["status"])}
              {f"<div class='tracker-note'>{html.escape(ticket['internal_note'])}</div>" if ticket['internal_note'] else ""}
              {f"<div class='tracker-note canceled-note'>Canceled: {html.escape(ticket['cancel_reason'])}</div>" if ticket['cancel_reason'] else ""}
              <div class="ticket-actions">{''.join(actions) if actions else "<span class='subtle'>No dispatch action needed.</span>"}</div>
            </article>
            """
        )
    block_cards = []
    for block in blocks:
        block_tickets = block_ticket_map.get(block["id"], [])
        active_count = block["active_ticket_count"]
        can_submit = block["status"] == "OPEN" and active_count == BLOCK_SIZE
        block_actions = ""
        if block["status"] == "OPEN":
            block_actions = f"""
            <form method="post" action="/orders/update" class="action-stack">
              <input type="hidden" name="order_id" value="{block_tickets[0]['id'] if block_tickets else 0}">
              <input type="hidden" name="block_id" value="{block["id"]}">
              <input type="hidden" name="action" value="submit_block">
              <label>Assign Driver<select name="driver_id" required><option value="">Choose driver</option>{driver_options}</select></label>
              <button type="submit" {'disabled' if not can_submit else ''}>Submit Block to Driver</button>
            </form>
            """
            if active_count != BLOCK_SIZE:
                block_actions += f"<div class='tracker-note warning-note'>Blocks must have exactly {BLOCK_SIZE} tickets before dispatch can submit them to a driver.</div>"
        else:
            block_actions = "<span class='subtle'>This block was already submitted to a driver.</span>"
        block_cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div><span class="eyebrow">Dispatch Block</span><h3>{html.escape(block["block_name"])}</h3></div>
                <span class="menu-count">{active_count}/{BLOCK_SIZE} tickets</span>
              </div>
              <div class="order-meta">
                <span>Status: {html.escape(block["status"].title())}</span>
                <span>Driver: {html.escape(block["driver_name"] or 'Unassigned')}</span>
                <span>Dispatcher: {html.escape(block["dispatcher_name"] or 'Dispatch')}</span>
              </div>
              <div class="item-pill-list">
                {''.join(f"<div class='item-pill'><strong>{html.escape(ticket['ticket_number'])}</strong><span>{html.escape(ticket['client_name'])}</span></div>" for ticket in block_tickets) or '<p>No tickets in this block yet.</p>'}
              </div>
              <div class="ticket-actions">{block_actions}</div>
            </article>
            """
        )
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Ready for Driver</span><strong>{sum(1 for ticket in tickets if ticket['status'] == 'READY_FOR_DISPATCH')}</strong></div>
      <div class="stat-card"><span>Open Blocks</span><strong>{sum(1 for block in blocks if block['status'] == 'OPEN')}</strong></div>
      <div class="stat-card"><span>Submitted Blocks</span><strong>{sum(1 for block in blocks if block['status'] == 'SUBMITTED')}</strong></div>
      <div class="stat-card"><span>Needs Review</span><strong>{sum(1 for ticket in tickets if ticket['status'] == 'REVIEW_REQUIRED')}</strong></div>
      <div class="stat-card"><span>Active Tickets</span><strong>{sum(1 for ticket in tickets if ticket['status'] != 'CANCELED')}</strong></div>
      <div class="stat-card"><span>Emergency Alerts</span><strong>{len(emergency_alerts)}</strong></div>
    </section>
    <section class="panel">
      <h2>Emergency Alerts</h2>
      <div class="order-card-grid">
        {''.join(
            f"""
            <article class="order-card {html.escape(emergency_meta(alert['category'].replace('EMERGENCY_', '').lower()).get('ui_class', ''))}">
              <div class="order-card-head">
                <div>
                  <span class="eyebrow">{html.escape(alert['priority'])} Priority</span>
                  <h3>{html.escape(alert['user_name'])}</h3>
                </div>
                <span class="badge badge-review_required">{html.escape(emergency_meta(alert['category'].replace('EMERGENCY_', '').lower()).get('label', alert['category']))}</span>
              </div>
              <div class="order-meta">
                <span>Ticket: {html.escape(alert['related_ticket_number'] or 'No ticket')}</span>
                <span>Opened By: {html.escape(alert['opened_by_name'])}</span>
                <span>Status: {html.escape(alert['status'])}</span>
              </div>
              <div class="reason-box">{html.escape(alert['reason'])}</div>
            </article>
            """
            for alert in emergency_alerts
        ) or '<p>No active emergency alerts.</p>'}
      </div>
    </section>
    <section class="panel"><h2>Dispatch Blocks</h2><div class="order-card-grid">{''.join(block_cards) if block_cards else '<p>No blocks created yet.</p>'}</div></section>
    <section class="panel"><h2>Dispatch Board</h2><div class="order-card-grid">{''.join(cards) if cards else '<p>No dispatch work waiting.</p>'}</div></section>
    {render_credit_issue_panel(connection)}
    """
    return page("Dispatcher Dashboard", body, user=user, message=message, level=level)


def render_picker_dashboard(connection, user, message=None, level="info"):
    tickets = ticket_rows(connection, "WHERE tickets.status IN ('PACKING', 'REVIEW_REQUIRED', 'READY_FOR_DISPATCH')", ())
    items_map = ticket_items_map(connection, [ticket["id"] for ticket in tickets])
    cards = []
    for ticket in tickets:
        actions = "<span class='subtle'>Waiting on another team member.</span>"
        if ticket["status"] == "PACKING":
            actions = f"""
            <div class="ticket-actions">
              <form method="post" action="/orders/update" class="action-stack">
                <input type="hidden" name="order_id" value="{ticket["id"]}">
                <input type="hidden" name="action" value="pack_order">
                <button type="submit">Mark Packed</button>
              </form>
              <form method="post" action="/orders/update" class="action-stack">
                <input type="hidden" name="order_id" value="{ticket["id"]}">
                <input type="hidden" name="action" value="send_review">
                <label>Review Reason<textarea name="reason" required placeholder="Why does this need a product change?"></textarea></label>
                <button type="submit">Send to Dispatcher Review</button>
              </form>
            </div>
            """
        cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div><span class="eyebrow">Ticket {html.escape(ticket["ticket_number"])}</span><h3>{html.escape(ticket["client_name"])}</h3></div>
                {status_badge(ticket["status"])}
              </div>
              <div class="order-meta">
                <span>Total Units: {ticket["total_units"]}</span>
                <span>Type: {html.escape(ticket["fulfillment_type"].title())}</span>
                <span>Dispatch: {html.escape(ticket["dispatcher_name"] or 'Open board')}</span>
                <span>Address: {html.escape(ticket["shipping_address"])}</span>
              </div>
              {render_item_list(items_map.get(ticket["id"], []))}
              {f"<div class='tracker-note warning-note'>Review reason: {html.escape(ticket['review_reason'])}</div>" if ticket['review_reason'] else ""}
              {actions}
            </article>
            """
        )
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Ready to Pack</span><strong>{sum(1 for ticket in tickets if ticket['status'] == 'PACKING')}</strong></div>
      <div class="stat-card"><span>Visible Tickets</span><strong>{len(tickets)}</strong></div>
    </section>
    <section class="panel"><h2>Packing Queue</h2><div class="order-card-grid">{''.join(cards) if cards else '<p>No packing work waiting.</p>'}</div></section>
    """
    return page("Picker Dashboard", body, user=user, message=message, level=level)


def render_driver_dashboard(connection, user, message=None, level="info"):
    tickets = ticket_rows(connection, "WHERE tickets.driver_id = ? AND tickets.status IN ('DRIVER_ASSIGNED', 'OUT_FOR_DELIVERY')", (user["id"],))
    items_map = ticket_items_map(connection, [ticket["id"] for ticket in tickets])
    block_names = sorted({ticket["delivery_block_name"] for ticket in tickets if ticket["delivery_block_name"]})
    cards = []
    for ticket in tickets:
        button = "Start Route" if ticket["status"] == "DRIVER_ASSIGNED" else "Mark Delivered"
        action = "start_route" if ticket["status"] == "DRIVER_ASSIGNED" else "deliver_order"
        cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div><span class="eyebrow">Ticket {html.escape(ticket["ticket_number"])}</span><h3>{html.escape(ticket["client_name"])}</h3></div>
                {status_badge(ticket["status"])}
              </div>
                <div class="order-meta">
                  <span>Total: {format_money(ticket["total_amount"])}</span>
                  <span>Type: {html.escape(ticket["fulfillment_type"].title())}</span>
                  <span>Address: {html.escape(ticket["shipping_address"])}</span>
                  <span>Block: {html.escape(ticket["delivery_block_name"] or 'Dispatch block pending')}</span>
                  <span>Dispatch: {html.escape(ticket["dispatcher_name"] or 'Dispatch board')}</span>
                  <span>Due: {format_money(max(0, ticket["total_amount"] - ticket["discount_amount"] - ticket["credit_applied"]))}</span>
                </div>
                {render_item_list(items_map.get(ticket["id"], []))}
                <div class="ticket-actions">
                  <form method="post" action="/orders/update" class="action-stack">
                    <input type="hidden" name="order_id" value="{ticket["id"]}">
                    <input type="hidden" name="action" value="{action}">
                    <button type="submit">{button}</button>
                  </form>
                  <div class="emergency-panel">
                    <strong>Driver Safety Resources</strong>
                    <div class="card-buttons emergency-buttons">
                      <form method="post" action="/orders/update" class="inline-form">
                        <input type="hidden" name="order_id" value="{ticket["id"]}">
                        <input type="hidden" name="action" value="driver_emergency">
                        <input type="hidden" name="emergency_type" value="medical_emergency">
                        <button type="submit" class="emergency-medical-button">Medical Emergency</button>
                      </form>
                      <form method="post" action="/orders/update" class="inline-form">
                        <input type="hidden" name="order_id" value="{ticket["id"]}">
                        <input type="hidden" name="action" value="driver_emergency">
                        <input type="hidden" name="emergency_type" value="car_accident">
                        <button type="submit" class="danger">Car Accident</button>
                      </form>
                      <form method="post" action="/orders/update" class="inline-form">
                        <input type="hidden" name="order_id" value="{ticket["id"]}">
                        <input type="hidden" name="action" value="driver_emergency">
                        <input type="hidden" name="emergency_type" value="robbery">
                        <button type="submit" class="danger">Robbery</button>
                      </form>
                      <form method="post" action="/orders/update" class="inline-form">
                        <input type="hidden" name="order_id" value="{ticket["id"]}">
                        <input type="hidden" name="action" value="driver_emergency">
                        <input type="hidden" name="emergency_type" value="traffic_stop">
                        <button type="submit" class="button ghost">Traffic Stop</button>
                      </form>
                    </div>
                    <div class="emergency-guide emergency-medical">
                      <strong>Medical emergency:</strong> Dispatch is notified. Proceed with your emergency and await for a message to your phone.
                    </div>
                    <div class="emergency-guide emergency-accident">
                      <strong>Car accident:</strong> Dial 911 now and begin the process of an accident report.
                    </div>
                    <div class="emergency-guide emergency-robbery">
                      <strong>Robbery:</strong> Just comply. Your life is not worth small amounts of anything. Return to base immediately when you are safe.
                    </div>
                    <div class="emergency-guide emergency-traffic">
                      <strong>Traffic stop:</strong> Do not panic. You are never traveling with an illegal amount of anything on you, so comply and you will be okay.
                    </div>
                  </div>
                </div>
            </article>
            """
        )
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Assigned Blocks</span><strong>{len(block_names)}</strong></div>
      <div class="stat-card"><span>Assigned Routes</span><strong>{sum(1 for ticket in tickets if ticket['status'] == 'DRIVER_ASSIGNED')}</strong></div>
      <div class="stat-card"><span>Live Deliveries</span><strong>{sum(1 for ticket in tickets if ticket['status'] == 'OUT_FOR_DELIVERY')}</strong></div>
    </section>
    <section class="panel"><h2>Driver Queue</h2><div class="order-card-grid">{''.join(cards) if cards else '<p>No routes assigned. Dispatch still needs to assign a driver.</p>'}</div></section>
    """
    return page("Driver Dashboard", body, user=user, message=message, level=level)


def render_admin_home(connection, user, message=None, level="info"):
    title = "Engineer Dashboard" if user["role"] == "helpdesk" else "Admin Dashboard"
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Total Accounts</span><strong>{connection.execute("SELECT COUNT(*) AS count FROM users").fetchone()["count"]}</strong></div>
      <div class="stat-card"><span>Menu Items</span><strong>{connection.execute("SELECT COUNT(*) AS count FROM products").fetchone()["count"]}</strong></div>
      <div class="stat-card"><span>Budhub Tickets</span><strong>{connection.execute("SELECT COUNT(*) AS count FROM tickets").fetchone()["count"]}</strong></div>
      <div class="stat-card"><span>Open Bag Lines</span><strong>{connection.execute("SELECT COUNT(*) AS count FROM cart_items").fetchone()["count"]}</strong></div>
    </section>
    <section class="admin-grid">
      <section class="panel">
        <span class="eyebrow">{html.escape(title)}</span>
        <h2>Budhub operations overview</h2>
        <div class="hero-actions"><a class="button" href="/admin">Open Admin Tools</a><a class="button ghost" href="/">View Customer Menu</a></div>
      </section>
      <section class="panel"><span class="eyebrow">Flow</span><h2>Bank verifies first, dispatch assigns drivers</h2><p>The workflow now follows the same order every time.</p></section>
    </section>
    """
    return page(title, body, user=user, message=message, level=level)


def render_admin_dashboard(connection, user, message=None, level="info"):
    users = connection.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
    products = connection.execute("SELECT * FROM products ORDER BY created_at DESC").fetchall()
    tickets = ticket_rows(connection)
    support = support_rows(connection)
    support_messages = support_messages_map(connection, [ticket["id"] for ticket in support])
    activity_logs = activity_log_rows(connection, "LIMIT 40")
    coupons = coupon_rows(connection)
    leafly_strains = leafly_strain_rows(connection)
    guest_help = guest_help_rows(connection)
    verification_queue = connection.execute(
        """
        SELECT * FROM users
        WHERE role = 'client' AND verification_status = 'PENDING_REVIEW'
        ORDER BY created_at ASC
        """
    ).fetchall()
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Total Accounts</span><strong>{len(users)}</strong></div>
      <div class="stat-card"><span>Menu Items</span><strong>{len(products)}</strong></div>
      <div class="stat-card"><span>Budhub Tickets</span><strong>{len(tickets)}</strong></div>
      <div class="stat-card"><span>Support Inbox</span><strong>{sum(1 for ticket in support if ticket['status'] != 'CLOSED')}</strong></div>
      <div class="stat-card"><span>Registration Help</span><strong>{sum(1 for request in guest_help if request['status'] != 'CLOSED')}</strong></div>
      <div class="stat-card"><span>ID Reviews</span><strong>{len(verification_queue)}</strong></div>
      <div class="stat-card"><span>Emergency Alerts</span><strong>{sum(1 for ticket in support if str(ticket['category']).startswith('EMERGENCY_') and ticket['status'] != 'CLOSED')}</strong></div>
    </section>
    <section class="admin-grid">
      <section class="panel">
        <h2>Create Team Member or Customer</h2>
        <form method="post" action="/users/create" class="form-grid">
          <label>Name<input type="text" name="name" required></label>
          <label>Email<input type="email" name="email" required></label>
          <label>Password<input type="password" name="password" minlength="6" required></label>
          <label>Role<select name="role"><option value="client">Customer</option><option value="banker">In-House Bank</option><option value="dispatcher">Dispatch Lead</option><option value="picker">Inventory Picker</option><option value="driver">Driver</option><option value="admin">Admin</option><option value="helpdesk">Budhub Helpdesk</option></select></label>
          <button type="submit">Create Account</button>
        </form>
      </section>
      <section class="panel">
        <h2>Add Menu Item</h2>
        <form method="post" action="/products/create" class="form-grid">
          <label>Name<input type="text" name="name" required></label>
          <label>Category
            <select name="category">
              <option value="Edibles">Edibles</option>
              <option value="Concentrates">Concentrates</option>
              <option value="Flower">Flower</option>
              <option value="General">General</option>
            </select>
          </label>
          <label>Sub Menu Label<input type="text" name="menu_group" placeholder="Example: Double Stuffed 7G, Diamonds, Syrup"></label>
          <label>Strain Type
            <select name="strain_type">
              <option value="Unspecified">Unspecified</option>
              <option value="Sativa">Sativa</option>
              <option value="Indica">Indica</option>
              <option value="Hybrid">Hybrid</option>
            </select>
          </label>
          <label>Leafly Strain
            <select name="leafly_strain_id">
              <option value="">Choose Leafly strain reference</option>
              {''.join(f"<option value='{strain['id']}'>{html.escape(strain['name'])} ({html.escape(strain['strain_type'])})</option>" for strain in leafly_strains)}
            </select>
          </label>
          <label>Price<input type="number" name="price" min="0.01" step="0.01" required></label>
          <label>Stock<input type="number" name="stock" min="0" required></label>
          <label>Description<textarea name="description" required></textarea></label>
          <button type="submit">Create Menu Item</button>
        </form>
      </section>
    </section>
    <section class="admin-grid">
      <section class="panel">
        <h2>Create Coupon</h2>
        <form method="post" action="/coupons/create" class="form-grid">
          <label>Code<input type="text" name="code" required></label>
          <label>Type<select name="discount_type"><option value="FLAT">Flat Amount</option><option value="PERCENT">Percent</option></select></label>
          <label>Value<input type="number" name="discount_value" min="0.01" step="0.01" required></label>
          <button type="submit">Create Coupon</button>
        </form>
        <div class="order-card-grid">
          {''.join(f"<div class='item-pill'><strong>{html.escape(coupon['code'])}</strong><span>{html.escape(coupon['discount_type'])} {coupon['discount_value']}</span><span>{'Active' if coupon['active'] else 'Inactive'}</span></div>" for coupon in coupons) or '<p>No coupons yet.</p>'}
        </div>
      </section>
      {render_credit_issue_panel(connection)}
    </section>
    <section class="panel">
      <h2>Latest Budhub Tickets</h2>
      <table>
        <thead><tr><th>Ticket</th><th>Customer</th><th>Status</th><th>Total</th></tr></thead>
        <tbody>{''.join(f"<tr><td>{html.escape(ticket['ticket_number'])}</td><td>{html.escape(ticket['client_name'])}</td><td>{status_badge(ticket['status'])}</td><td>{format_money(ticket['total_amount'])}</td></tr>" for ticket in tickets[:8]) or '<tr><td colspan=\"4\">No tickets yet.</td></tr>'}</tbody>
      </table>
    </section>
    <section class="panel">
      <h2>ID Verification Queue</h2>
      <div class="order-card-grid">
        {''.join(
            f'''
            <article class="order-card">
              <div class="order-card-head">
                <div><span class="eyebrow">{html.escape(account["email"])}</span><h3>{html.escape(account["name"])}</h3></div>
                <span class="badge badge-review_required">Pending Review</span>
              </div>
              <div class="order-meta">
                <span>Created: {html.escape(account["created_at"])}</span>
                <span>Status: {html.escape(account["verification_status"])}</span>
              </div>
              <div class="verification-grid">
                <a class="verification-card" href="{html.escape(account["id_front_path"] or "#")}" target="_blank"><img src="{html.escape(account["id_front_path"] or "")}" alt="ID front"><span>ID Front</span></a>
                <a class="verification-card" href="{html.escape(account["id_back_path"] or "#")}" target="_blank"><img src="{html.escape(account["id_back_path"] or "")}" alt="ID back"><span>ID Back</span></a>
                <a class="verification-card" href="{html.escape(account["id_selfie_path"] or "#")}" target="_blank"><img src="{html.escape(account["id_selfie_path"] or "")}" alt="Selfie holding ID"><span>Selfie With ID</span></a>
              </div>
              <form method="post" action="/users/verify" class="action-stack">
                <input type="hidden" name="user_id" value="{account["id"]}">
                <label>Admin Note<textarea name="note" placeholder="Optional approval note or required rejection note"></textarea></label>
                <div class="card-buttons">
                  <button type="submit" name="decision" value="approve">Approve Account</button>
                  <button type="submit" name="decision" value="reject" class="danger">Reject Account</button>
                </div>
              </form>
            </article>
            '''
            for account in verification_queue
        ) or '<p>No pending ID reviews.</p>'}
      </div>
    </section>
    <section class="admin-grid">
      <section class="panel">
        <h2>Account Controls</h2>
        <div class="order-card-grid">
          {''.join(
              f'''
              <article class="order-card">
                <div class="order-card-head">
                  <div><span class="eyebrow">{html.escape(account["email"])}</span><h3>{html.escape(account["name"])}</h3></div>
                  <span class="badge badge-{"delivered" if account["account_state"] == "ACTIVE" else "review_required"}">{html.escape(account["account_state"].title())}</span>
                </div>
                <div class="order-meta">
                  <span>Role: {html.escape(ROLE_LABELS.get(account["role"], account["role"]))}</span>
                  <span>Reason: {html.escape(account["account_reason"] or "None")}</span>
                </div>
                <form method="post" action="/users/update" class="action-stack">
                  <input type="hidden" name="user_id" value="{account["id"]}">
                  <label>Account Action<select name="account_state"><option value="ACTIVE">Active</option><option value="LOCKED">Lock</option><option value="SUSPENDED">Suspend</option><option value="BANNED">Ban</option></select></label>
                  <label>Reason<textarea name="reason" required placeholder="Reason for account state change"></textarea></label>
                  <button type="submit">Update Account</button>
                </form>
              </article>
              '''
              for account in users if account["role"] != "admin"
          ) or '<p>No accounts available.</p>'}
        </div>
      </section>
      <section class="panel">
        <h2>Support Inbox</h2>
        <div class="order-card-grid">
          {''.join(
              f'''
              <article class="order-card">
                <div class="order-card-head">
                  <div><span class="eyebrow">{html.escape(ticket["category"])}</span><h3>{html.escape(ticket["user_name"])}</h3></div>
                  <span class="badge badge-{"placed" if ticket["status"] != "CLOSED" else "delivered"}">{html.escape(ticket["status"].title())}</span>
                </div>
                <div class="order-meta">
                  <span>User: {html.escape(ticket["user_email"])}</span>
                  <span>Opened By: {html.escape(ticket["opened_by_name"])}</span>
                  <span>Assigned To: {html.escape(ticket["assigned_to_name"] or "Unassigned")}</span>
                  <span>Priority: {html.escape(ticket["priority"])}</span>
                  <span>Ticket: {html.escape(ticket["related_ticket_number"] or "N/A")}</span>
                </div>
                <div class="reason-box {'emergency-medical' if ticket['category']=='EMERGENCY_MEDICAL_EMERGENCY' else 'emergency-accident' if ticket['category']=='EMERGENCY_CAR_ACCIDENT' else 'emergency-robbery' if ticket['category']=='EMERGENCY_ROBBERY' else 'emergency-traffic' if ticket['category']=='EMERGENCY_TRAFFIC_STOP' else ''}">{html.escape(ticket["reason"])}</div>
                <div class="item-pill-list">
                  {''.join(f"<div class='item-pill'><strong>{html.escape(message['author_name'])}</strong><span>{html.escape(message['message'])}</span></div>" for message in support_messages.get(ticket['id'], [])) or '<p>No replies yet.</p>'}
                </div>
                <form method="post" action="/support/update" class="action-stack">
                  <input type="hidden" name="ticket_id" value="{ticket["id"]}">
                  <label>Assign To
                    <select name="assigned_to">
                      <option value="">Unassigned</option>
                      {''.join(f"<option value='{account['id']}' {'selected' if ticket['assigned_to'] == account['id'] else ''}>{html.escape(account['name'])}</option>" for account in users if account['role'] in {'admin', 'helpdesk'})}
                    </select>
                  </label>
                  <label>Review Status<select name="status"><option value="OPEN">Open</option><option value="REVIEWED">Reviewed</option><option value="CLOSED">Closed</option></select></label>
                  <label>Reply<textarea name="reply_message" placeholder="Reply to the user from admin/helpdesk"></textarea></label>
                  <label>Resolution Note<textarea name="resolution_note" placeholder="Optional review note"></textarea></label>
                  <button type="submit">Update Support Ticket</button>
                </form>
              </article>
              '''
              for ticket in support
          ) or '<p>No support tickets in the inbox.</p>'}
        </div>
      </section>
    </section>
    <section class="panel">
      <h2>Registration Help Requests</h2>
      <div class="order-card-grid">
        {''.join(f"<article class='order-card'><div class='order-card-head'><div><span class='eyebrow'>{html.escape(request['email'])}</span><h3>{html.escape(request['name'])}</h3></div><span class='menu-count'>{html.escape(request['status'])}</span></div><div class='reason-box'>{html.escape(request['issue'])}</div><form method='post' action='/guest-help/update' class='action-stack'><input type='hidden' name='request_id' value='{request['id']}'><label>Status<select name='status'><option value='OPEN'>Open</option><option value='REVIEWED'>Reviewed</option><option value='CLOSED'>Closed</option></select></label><label>Response Note<textarea name='response_note' placeholder='Internal follow-up or response summary'></textarea></label><button type='submit'>Update Request</button></form></article>" for request in guest_help) or '<p>No registration help requests yet.</p>'}
      </div>
    </section>
    <section class="panel">
      <h2>Account Activity Log</h2>
      <div class="order-card-grid">
        {''.join(f"<article class='order-card'><div class='order-card-head'><div><span class='eyebrow'>{html.escape(log['actor_role'] or 'System')}</span><h3>{html.escape(log['actor_name'] or 'System')}</h3></div><span class='menu-count'>{html.escape(log['created_at'])}</span></div><div class='order-meta'><span>Action: {html.escape(log['action'])}</span><span>Target: {html.escape(log['target_user_name'] or 'N/A')}</span></div><div class='reason-box'>{html.escape(log['details'] or 'No extra details provided.')}</div></article>" for log in activity_logs) or '<p>No activity logged yet.</p>'}
      </div>
    </section>
    """
    return page("Admin Tools", body, user=user, message=message, level=level)


def render_helpdesk_dashboard(connection, user, message=None, level="info"):
    tickets = support_rows(connection, "WHERE support_tickets.user_id = ? OR support_tickets.opened_by = ?", (user["id"], user["id"]))
    message_map = support_messages_map(connection, [ticket["id"] for ticket in tickets])
    cards = []
    for ticket in tickets:
        replies = "".join(
            f"<div class='item-pill'><strong>{html.escape(entry['author_name'])}</strong><span>{html.escape(entry['message'])}</span></div>"
            for entry in message_map.get(ticket["id"], [])
        ) or "<p>No replies yet.</p>"
        reply_form = ""
        if ticket["status"] != "CLOSED":
            reply_form = f"""
            <form method="post" action="/support/reply" class="action-stack">
              <input type="hidden" name="ticket_id" value="{ticket["id"]}">
              <label>Reply<textarea name="message" required placeholder="Add more details or reply to support"></textarea></label>
              <button type="submit">Send Reply</button>
            </form>
            """
        cards.append(
            f"""
            <article class="order-card">
              <div class="order-card-head">
                <div><span class="eyebrow">{html.escape(ticket["category"])}</span><h3>{html.escape(ticket["subject"] or "Support Ticket")}</h3></div>
                <span class="menu-count">{html.escape(ticket["status"])}</span>
              </div>
              <div class="reason-box">{html.escape(ticket["reason"])}</div>
              <div class="item-pill-list">{replies}</div>
              {reply_form}
            </article>
            """
        )
    body = f"""
    <section class="stats-row">
      <div class="stat-card"><span>Open Help Tickets</span><strong>{sum(1 for ticket in tickets if ticket['status'] != 'CLOSED')}</strong></div>
      <div class="stat-card"><span>Total Messages</span><strong>{sum(len(message_map.get(ticket['id'], [])) for ticket in tickets)}</strong></div>
    </section>
    <section class="admin-grid">
      <section class="panel">
        <h2>Open Help Ticket</h2>
        <form method="post" action="/support/create" class="form-grid">
          <label>Subject<input type="text" name="subject" required placeholder="Login issue, order issue, account verification..."></label>
          <label>Category<select name="category"><option value="GENERAL_HELP">General Help</option><option value="ACCOUNT_HELP">Account Help</option><option value="ORDER_HELP">Order Help</option><option value="TECHNICAL_HELP">Technical Help</option></select></label>
          <label>Priority<select name="priority"><option value="NORMAL">Normal</option><option value="HIGH">High</option></select></label>
          <label>Issue<textarea name="reason" required placeholder="Explain what is happening and what you need help with."></textarea></label>
          <button type="submit">Send to Budhub Helpdesk</button>
        </form>
      </section>
      <section class="panel">
        <h2>How Helpdesk Works</h2>
        <p>Use this dashboard to report account, order, or technical issues. Admin and Budhub Helpdesk can review the thread, reply, and close the issue when it is resolved.</p>
      </section>
    </section>
    <section class="panel">
      <h2>Your Help Conversations</h2>
      <div class="order-card-grid">{''.join(cards) if cards else '<p>No help tickets yet.</p>'}</div>
    </section>
    """
    return page("Budhub Help", body, user=user, message=message, level=level)


def render_dashboard(connection, user, message=None, level="info"):
    if user["role"] == "client":
        return render_client_dashboard(connection, user, message, level)
    if user["role"] == "helpdesk":
        return render_helpdesk_dashboard(connection, user, message, level)
    if user["role"] == "banker":
        return render_banker_dashboard(connection, user, message, level)
    if user["role"] == "dispatcher":
        return render_dispatcher_dashboard(connection, user, message, level)
    if user["role"] == "picker":
        return render_picker_dashboard(connection, user, message, level)
    if user["role"] == "driver":
        return render_driver_dashboard(connection, user, message, level)
    if user["role"] == "admin":
        return render_admin_home(connection, user, message, level)
    return page("Dashboard", "<section class='panel'><p>Unknown role.</p></section>", user=user)


def require_user(start_response, user):
    if user:
        return None
    return redirect(start_response, "/login")


def require_role(start_response, user, roles):
    if not user:
        return redirect(start_response, "/login")
    if user["role"] == "helpdesk":
        return None
    if user["role"] not in roles:
        return text_response(start_response, page("Access Denied", "<section class='panel'><p>You do not have access to that page.</p></section>", user=user), status="403 Forbidden")
    return None


def update_ticket(connection, ticket_id, **fields):
    assignments = [f"{key} = ?" for key in fields]
    values = list(fields.values())
    assignments.append("updated_at = ?")
    values.append(now_iso())
    values.append(ticket_id)
    connection.execute(f"UPDATE tickets SET {', '.join(assignments)} WHERE id = ?", values)


def refresh_delivery_block_status(connection, block_id):
    if not block_id:
        return
    block = connection.execute("SELECT * FROM delivery_blocks WHERE id = ?", (block_id,)).fetchone()
    if not block:
        return
    count_row = connection.execute(
        "SELECT COUNT(*) AS count FROM tickets WHERE delivery_block_id = ? AND status NOT IN ('CANCELED', 'DELIVERED')",
        (block_id,),
    ).fetchone()
    active_count = count_row["count"] if count_row else 0
    if active_count == 0:
        connection.execute("DELETE FROM delivery_blocks WHERE id = ?", (block_id,))
        return
    if block["status"] == "SUBMITTED":
        return
    connection.execute(
        "UPDATE delivery_blocks SET updated_at = ? WHERE id = ?",
        (now_iso(), block_id),
    )


def handle_login(environ, start_response, connection):
    data = read_post_data(environ)
    user = connection.execute("SELECT * FROM users WHERE email = ?", (data.get("email", "").lower(),)).fetchone()
    if not user or user["password_hash"] != hash_password(data.get("password", "")):
        return text_response(start_response, page("Login", login_form("Incorrect email or password.")))
    log_activity(connection, user, "LOGIN", "Signed into Budhub.")
    token = create_session(connection, user["id"])
    connection.commit()
    return redirect(start_response, "/dashboard", f"{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax")


def handle_register(environ, start_response, connection):
    content_type = environ.get("CONTENT_TYPE", "")
    if "multipart/form-data" in content_type:
        data, files = read_multipart_form(environ)
    else:
        data = read_post_data(environ)
        files = {}
    email = data.get("email", "").lower()
    password = data.get("password", "")
    if len(password) < 6:
        return text_response(start_response, page("Register", register_form("Password must be at least 6 characters long.")))
    if connection.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone():
        return text_response(start_response, page("Register", register_form("That email already has an account.")))
    required_files = {"id_front", "id_back", "id_selfie"}
    if not all(key in files for key in required_files):
        return text_response(start_response, page("Register", register_form("ID front, ID back, and selfie holding ID are all required.")))
    cursor = connection.execute(
        """
        INSERT INTO users (
            name, email, password_hash, role, account_state, verification_status, verification_note, created_at
        ) VALUES (?, ?, ?, 'client', 'PENDING_VERIFICATION', 'PENDING_REVIEW', ?, ?)
        """,
        (data.get("name", ""), email, hash_password(password), "Awaiting ID verification.", now_iso()),
    )
    user_id = cursor.lastrowid
    try:
        id_front_path = save_verification_upload(user_id, "front", files["id_front"])
        id_back_path = save_verification_upload(user_id, "back", files["id_back"])
        id_selfie_path = save_verification_upload(user_id, "selfie", files["id_selfie"])
    except ValueError as exc:
        connection.execute("DELETE FROM users WHERE id = ?", (user_id,))
        connection.commit()
        return text_response(start_response, page("Register", register_form(str(exc))))
    connection.execute(
        """
        UPDATE users
        SET id_front_path = ?, id_back_path = ?, id_selfie_path = ?
        WHERE id = ?
        """,
        (id_front_path, id_back_path, id_selfie_path, user_id),
    )
    created_user = connection.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    log_activity(connection, created_user, "REGISTER", "Created a customer account and uploaded verification documents.", target_user_id=user_id)
    connection.commit()
    return redirect(start_response, "/login?message=Account created and waiting for ID verification")


def handle_create_product(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    category = data.get("category", "General") or "General"
    menu_group, default_strain_type = infer_product_metadata(data.get("name", ""), category, data.get("description", ""))
    selected_leafly = None
    leafly_id = int(data.get("leafly_strain_id", "0") or "0")
    if leafly_id:
        selected_leafly = connection.execute("SELECT * FROM leafly_strains WHERE id = ?", (leafly_id,)).fetchone()
    strain_type = normalize_strain_type(data.get("strain_type") or default_strain_type or "Unspecified")
    if selected_leafly and category in {"Flower", "Concentrates"}:
        strain_type = normalize_strain_type(selected_leafly["strain_type"] or strain_type)
    connection.execute(
        "INSERT INTO products (name, category, description, image_url, source_url, leafly_strain_name, price, stock, menu_group, strain_type, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            data.get("name", ""),
            category,
            data.get("description", ""),
            selected_leafly["image_url"] if selected_leafly else None,
            selected_leafly["source_url"] if selected_leafly else None,
            selected_leafly["name"] if selected_leafly else None,
            float(data.get("price", "0")),
            int(data.get("stock", "0")),
            data.get("menu_group", "").strip() or menu_group,
            "" if category not in {"Flower", "Concentrates"} else strain_type,
            now_iso(),
        ),
    )
    log_activity(connection, user, "CREATE_PRODUCT", f"Created catalog item {data.get('name', '')}.")
    connection.commit()
    return redirect(start_response, "/admin?message=Menu item created")


def handle_create_coupon(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    code = normalize_coupon_code(data.get("code", ""))
    if not code:
        return redirect(start_response, "/admin?message=Coupon code is required")
    try:
        connection.execute(
            """
            INSERT INTO coupons (code, discount_type, discount_value, active, created_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (code, data.get("discount_type", "FLAT"), float(data.get("discount_value", "0")), now_iso()),
        )
        connection.commit()
    except sqlite3.IntegrityError:
        return redirect(start_response, "/admin?message=Coupon code already exists")
    return redirect(start_response, "/admin?message=Coupon created")


def handle_issue_credit(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin", "dispatcher", "banker"})
    if gate:
        return gate
    data = read_post_data(environ)
    target_id = int(data.get("user_id", "0"))
    amount = round(float(data.get("amount", "0")), 2)
    note = data.get("note", "").strip()
    target = connection.execute("SELECT * FROM users WHERE id = ?", (target_id,)).fetchone()
    if not target:
        return redirect(start_response, "/dashboard?message=Customer not found")
    if amount <= 0 or not note:
        return redirect(start_response, "/dashboard?message=Credit amount and note are required")
    connection.execute("UPDATE users SET credit_balance = credit_balance + ? WHERE id = ?", (amount, target_id))
    connection.execute(
        """
        INSERT INTO credit_ledger (user_id, issued_by, amount, note, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (target_id, user["id"], amount, note, now_iso()),
    )
    connection.commit()
    destination = "/admin" if user["role"] == "admin" else "/dashboard"
    return redirect(start_response, f"{destination}?message=Credits issued")


def handle_create_user(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    email = data.get("email", "").lower()
    if connection.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone():
        return redirect(start_response, "/admin?message=Account already exists")
    connection.execute(
        """
        INSERT INTO users (
            name, email, password_hash, role, account_state, verification_status, verified_at, created_at
        ) VALUES (?, ?, ?, ?, 'ACTIVE', 'VERIFIED', ?, ?)
        """,
        (data.get("name", ""), email, hash_password(data.get("password", "")), data.get("role", "client"), now_iso(), now_iso()),
    )
    created = connection.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    log_activity(connection, user, "CREATE_USER", f"Created {data.get('role', 'client')} account for {email}.", target_user_id=created["id"] if created else None)
    connection.commit()
    return redirect(start_response, "/admin?message=Account created")


def handle_create_support_ticket(environ, start_response, connection, user):
    gate = require_user(start_response, user)
    if gate:
        return gate
    data = read_post_data(environ)
    reason = data.get("reason", "").strip()
    subject = data.get("subject", "").strip()
    if not subject or not reason:
        return redirect(start_response, "/help?message=Subject and issue details are required")
    cursor = connection.execute(
        """
        INSERT INTO support_tickets (user_id, opened_by, category, subject, priority, related_ticket_id, reason, status, resolution_note, assigned_to, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, NULL, ?, 'OPEN', '', NULL, ?, ?)
        """,
        (user["id"], user["id"], data.get("category", "GENERAL_HELP"), subject, data.get("priority", "NORMAL"), reason, now_iso(), now_iso()),
    )
    ticket_id = cursor.lastrowid
    connection.execute(
        "INSERT INTO support_messages (support_ticket_id, author_id, message, created_at) VALUES (?, ?, ?, ?)",
        (ticket_id, user["id"], reason, now_iso()),
    )
    log_activity(connection, user, "OPEN_SUPPORT_TICKET", f"Opened support ticket: {subject}.", target_user_id=user["id"])
    connection.commit()
    return redirect(start_response, "/help?message=Help ticket created")


def handle_guest_help_request(environ, start_response, connection):
    data = read_post_data(environ)
    if not data.get("name", "").strip() or not data.get("email", "").strip() or not data.get("issue", "").strip():
        return redirect(start_response, "/register?message=Name, email, and issue are required")
    connection.execute(
        """
        INSERT INTO guest_help_requests (name, email, issue, status, response_note, created_at, updated_at)
        VALUES (?, ?, ?, 'OPEN', '', ?, ?)
        """,
        (data.get("name", "").strip(), data.get("email", "").strip().lower(), data.get("issue", "").strip(), now_iso(), now_iso()),
    )
    connection.commit()
    return redirect(start_response, "/register?message=Registration help request sent")


def handle_support_reply(environ, start_response, connection, user):
    gate = require_user(start_response, user)
    if gate:
        return gate
    data = read_post_data(environ)
    ticket_id = int(data.get("ticket_id", "0"))
    ticket = connection.execute("SELECT * FROM support_tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket:
        return redirect(start_response, "/help?message=Support ticket not found")
    if user["role"] not in {"admin", "helpdesk"} and ticket["user_id"] != user["id"]:
        return redirect(start_response, "/help?message=That support ticket is not yours")
    message = data.get("message", "").strip()
    if not message:
        return redirect(start_response, "/help?message=Reply message is required")
    connection.execute(
        "INSERT INTO support_messages (support_ticket_id, author_id, message, created_at) VALUES (?, ?, ?, ?)",
        (ticket_id, user["id"], message, now_iso()),
    )
    connection.execute(
        "UPDATE support_tickets SET status = CASE WHEN status = 'CLOSED' THEN 'REVIEWED' ELSE status END, updated_at = ? WHERE id = ?",
        (now_iso(), ticket_id),
    )
    log_activity(connection, user, "REPLY_SUPPORT_TICKET", f"Replied to support ticket #{ticket_id}.", target_user_id=ticket["user_id"])
    connection.commit()
    return redirect(start_response, "/help?message=Reply sent")


def handle_add_to_cart(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"client"})
    if gate:
        return gate
    data = read_post_data(environ)
    product_id = int(data.get("product_id", "0"))
    quantity = int(data.get("quantity", "1"))
    return_to = data.get("return_to", "/") or "/"
    product = connection.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    if not product:
        return redirect_with_message(start_response, return_to, "Menu item not found")
    if quantity < 1 or quantity > product["stock"]:
        return redirect_with_message(start_response, return_to, "Please choose a valid quantity")
    existing = connection.execute("SELECT * FROM cart_items WHERE user_id = ? AND product_id = ?", (user["id"], product_id)).fetchone()
    if existing:
        connection.execute("UPDATE cart_items SET quantity = ? WHERE id = ?", (min(existing["quantity"] + quantity, product["stock"]), existing["id"]))
    else:
        connection.execute("INSERT INTO cart_items (user_id, product_id, quantity, created_at) VALUES (?, ?, ?, ?)", (user["id"], product_id, quantity, now_iso()))
    log_activity(connection, user, "ADD_TO_BAG", f"Added {quantity} of {product['name']} to the bag.", target_user_id=user["id"])
    connection.commit()
    return redirect_with_message(start_response, return_to, "Added to bag")


def handle_remove_from_cart(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"client"})
    if gate:
        return gate
    data = read_post_data(environ)
    product_id = int(data.get("product_id", "0"))
    return_to = data.get("return_to", "/#bag-widget") or "/#bag-widget"
    product = connection.execute("SELECT * FROM products WHERE id = ?", (product_id,)).fetchone()
    connection.execute("DELETE FROM cart_items WHERE user_id = ? AND product_id = ?", (user["id"], product_id))
    if product:
        log_activity(connection, user, "REMOVE_FROM_BAG", f"Removed {product['name']} from the bag.", target_user_id=user["id"])
    connection.commit()
    return redirect_with_message(start_response, return_to, "Item removed")


def handle_create_order(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"client"})
    if gate:
        return gate
    data = read_post_data(environ)
    fulfillment_type = data.get("fulfillment_type", "DELIVERY")
    shipping_address = data.get("shipping_address", "").strip()
    if fulfillment_type == "DELIVERY" and not shipping_address:
        return text_response(start_response, order_form(connection, int(data.get("product_id", "0")), user, "Delivery address is required for delivery orders."))
    if fulfillment_type == "PICKUP" and not shipping_address:
        shipping_address = "In-store pickup"
    try:
        ticket_id = create_ticket(
            connection,
            user["id"],
            [{"product_id": int(data.get("product_id", "0")), "quantity": int(data.get("quantity", "1"))}],
            shipping_address,
            data.get("customer_note", "").strip(),
            fulfillment_type,
            data.get("coupon_code", ""),
            data.get("use_credits") == "yes",
        )
    except ValueError as exc:
        return text_response(start_response, order_form(connection, int(data.get("product_id", "0")), user, str(exc)))
    log_activity(connection, user, "CREATE_ORDER", f"Created order ticket #{ticket_id}.", target_user_id=user["id"])
    connection.commit()
    return redirect(start_response, "/dashboard?message=Order placed")


def handle_cart_checkout(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"client"})
    if gate:
        return gate
    data = read_post_data(environ)
    return_to = data.get("return_to", "/#bag-widget") or "/#bag-widget"
    items = cart_items_for_user(connection, user["id"])
    if not items:
        return redirect_with_message(start_response, return_to, "Your bag is empty")
    fulfillment_type = data.get("fulfillment_type", "DELIVERY")
    shipping_address = data.get("shipping_address", "").strip()
    if fulfillment_type == "DELIVERY" and not shipping_address:
        return redirect_with_message(start_response, return_to, "Delivery address is required")
    if fulfillment_type == "PICKUP" and not shipping_address:
        shipping_address = "In-store pickup"
    try:
        ticket_id = create_ticket(
            connection,
            user["id"],
            [{"product_id": item["product_id"], "quantity": item["quantity"]} for item in items],
            shipping_address,
            data.get("customer_note", "").strip(),
            fulfillment_type,
            data.get("coupon_code", ""),
            data.get("use_credits") == "yes",
        )
    except ValueError as exc:
        return redirect_with_message(start_response, return_to, str(exc))
    connection.execute("DELETE FROM cart_items WHERE user_id = ?", (user["id"],))
    log_activity(connection, user, "CHECKOUT_BAG", f"Created grouped order ticket #{ticket_id}.", target_user_id=user["id"])
    connection.commit()
    return redirect(start_response, "/dashboard?message=Grouped Budhub ticket created")


def handle_update_order(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"banker", "dispatcher", "picker", "driver", "client"})
    if gate:
        return gate
    data = read_post_data(environ)
    ticket_id = int(data.get("order_id", "0"))
    action = data.get("action", "")
    ticket = single_ticket(connection, ticket_id)
    if not ticket:
        return redirect(start_response, "/dashboard?message=Ticket not found")

    if user["role"] == "banker":
        if action == "verify_payment" and ticket["payment_status"] == "PENDING" and ticket["status"] not in {"CANCELED", "DELIVERED"}:
            update_ticket(connection, ticket_id, payment_status="VERIFIED", banker_id=user["id"])
            connection.commit()
            return redirect(start_response, "/dashboard?message=Payment verified")
        return redirect(start_response, "/dashboard?message=That bank action is not allowed")

    if user["role"] == "client":
        if ticket["client_id"] != user["id"]:
            return redirect(start_response, "/dashboard?message=That order is not yours")
        if action == "client_cancel":
            if ticket["status"] in {"DELIVERED", "CANCELED", "OUT_FOR_DELIVERY"}:
                return redirect(start_response, "/dashboard?message=This order can no longer be canceled online")
            reason = data.get("reason", "").strip()
            if not reason:
                return redirect(start_response, "/dashboard?message=Cancel reason is required")
            release_ticket_stock(connection, ticket_id)
            prior_block_id = ticket["delivery_block_id"]
            update_ticket(connection, ticket_id, status="CANCELED", cancel_reason=reason, delivery_block_id=None)
            refresh_delivery_block_status(connection, prior_block_id)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Order canceled")
        return redirect(start_response, "/dashboard?message=That customer action is not allowed")

    if user["role"] == "picker":
        if action == "pack_order" and ticket["status"] == "PACKING":
            next_status = "READY_FOR_PICKUP" if ticket["fulfillment_type"] == "PICKUP" else "READY_FOR_DISPATCH"
            update_ticket(connection, ticket_id, status=next_status, picker_id=user["id"], review_reason=None)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Packed and returned to dispatch")
        if action == "send_review" and ticket["status"] == "PACKING":
            reason = data.get("reason", "").strip()
            if not reason:
                return redirect(start_response, "/dashboard?message=Review reason is required")
            release_ticket_stock(connection, ticket_id)
            update_ticket(connection, ticket_id, status="REVIEW_REQUIRED", picker_id=user["id"], review_reason=reason, driver_id=None)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Ticket sent to dispatcher review")
        return redirect(start_response, "/dashboard?message=That picker action is not allowed")

    if user["role"] == "dispatcher":
        if action == "complete_pickup" and ticket["status"] == "READY_FOR_PICKUP":
            if ticket["payment_status"] != "VERIFIED":
                return redirect(start_response, "/dashboard?message=Pickup cannot be completed until payment is verified")
            update_ticket(connection, ticket_id, status="DELIVERED", dispatcher_id=user["id"], internal_note="Customer picked up in person.")
            connection.commit()
            return redirect(start_response, "/dashboard?message=Pickup completed")
        if action == "create_block_for_ticket" and ticket["status"] == "READY_FOR_DISPATCH":
            block_id = create_delivery_block(connection, user["id"])
            update_ticket(connection, ticket_id, delivery_block_id=block_id, dispatcher_id=user["id"], internal_note=f"Assigned to block {connection.execute('SELECT block_name FROM delivery_blocks WHERE id = ?', (block_id,)).fetchone()['block_name']}")
            refresh_delivery_block_status(connection, block_id)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Ticket added to new block")
        if action == "assign_to_block" and ticket["status"] == "READY_FOR_DISPATCH":
            block_id = int(data.get("block_id", "0"))
            block = connection.execute("SELECT * FROM delivery_blocks WHERE id = ?", (block_id,)).fetchone()
            if not block or block["status"] != "OPEN":
                return redirect(start_response, "/dashboard?message=Choose a valid open block")
            block_ticket_count = connection.execute(
                "SELECT COUNT(*) AS count FROM tickets WHERE delivery_block_id = ? AND status NOT IN ('CANCELED', 'DELIVERED')",
                (block_id,),
            ).fetchone()["count"]
            if block_ticket_count >= BLOCK_SIZE:
                return redirect(start_response, f"/dashboard?message=That block already has {BLOCK_SIZE} tickets")
            update_ticket(connection, ticket_id, delivery_block_id=block_id, dispatcher_id=user["id"], internal_note=f"Assigned to block {block['block_name']}")
            refresh_delivery_block_status(connection, block_id)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Ticket assigned to block")
        if action == "submit_block" and ticket["status"] == "READY_FOR_DISPATCH":
            block_id = int(data.get("block_id", "0"))
            block = connection.execute("SELECT * FROM delivery_blocks WHERE id = ?", (block_id,)).fetchone()
            driver = connection.execute("SELECT * FROM users WHERE id = ? AND role = 'driver'", (int(data.get("driver_id", "0")),)).fetchone()
            if not block or block["status"] != "OPEN":
                return redirect(start_response, "/dashboard?message=Choose a valid open block")
            driver = connection.execute("SELECT * FROM users WHERE id = ? AND role = 'driver'", (int(data.get("driver_id", "0")),)).fetchone()
            if not driver:
                return redirect(start_response, "/dashboard?message=Choose a valid driver")
            block_tickets = connection.execute(
                "SELECT id FROM tickets WHERE delivery_block_id = ? AND status = 'READY_FOR_DISPATCH' ORDER BY created_at ASC, id ASC",
                (block_id,),
            ).fetchall()
            if len(block_tickets) != BLOCK_SIZE:
                return redirect(start_response, f"/dashboard?message=Blocks need exactly {BLOCK_SIZE} ready tickets before dispatch can submit them")
            connection.execute(
                "UPDATE delivery_blocks SET driver_id = ?, status = 'SUBMITTED', submitted_at = ?, updated_at = ? WHERE id = ?",
                (driver["id"], now_iso(), now_iso(), block_id),
            )
            block_name = block["block_name"]
            for block_ticket in block_tickets:
                update_ticket(
                    connection,
                    block_ticket["id"],
                    status="DRIVER_ASSIGNED",
                    driver_id=driver["id"],
                    dispatcher_id=user["id"],
                    internal_note=f"Submitted in {block_name}",
                )
            connection.commit()
            return redirect(start_response, "/dashboard?message=Block submitted to driver")
        if action == "pull_back" and ticket["status"] in {"DRIVER_ASSIGNED", "OUT_FOR_DELIVERY"}:
            reason = data.get("reason", "").strip()
            if not reason:
                return redirect(start_response, "/dashboard?message=Pull back reason is required")
            prior_block_id = ticket["delivery_block_id"]
            update_ticket(connection, ticket_id, status="READY_FOR_DISPATCH", dispatcher_id=user["id"], driver_id=None, delivery_block_id=None, internal_note=reason)
            refresh_delivery_block_status(connection, prior_block_id)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Ticket pulled back to dispatch")
        if action == "cancel_order" and ticket["status"] not in {"DELIVERED", "CANCELED"}:
            reason = data.get("reason", "").strip()
            if not reason:
                return redirect(start_response, "/dashboard?message=Cancel reason is required")
            release_ticket_stock(connection, ticket_id)
            prior_block_id = ticket["delivery_block_id"]
            update_ticket(connection, ticket_id, status="CANCELED", dispatcher_id=user["id"], driver_id=None, delivery_block_id=None, cancel_reason=reason)
            refresh_delivery_block_status(connection, prior_block_id)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Ticket canceled")
        if action == "resolve_review" and ticket["status"] == "REVIEW_REQUIRED":
            items = ticket_items_map(connection, [ticket_id]).get(ticket_id, [])
            for item in items:
                replacement_id = int(data.get(f"replacement_{item['id']}", str(item["product_id"])) or item["product_id"])
                replacement = connection.execute("SELECT * FROM products WHERE id = ?", (replacement_id,)).fetchone()
                if not replacement or replacement["stock"] < item["quantity"]:
                    return redirect(start_response, "/dashboard?message=A replacement item does not have enough stock")
            for item in items:
                replacement_id = int(data.get(f"replacement_{item['id']}", str(item["product_id"])) or item["product_id"])
                replacement = connection.execute("SELECT * FROM products WHERE id = ?", (replacement_id,)).fetchone()
                connection.execute("UPDATE ticket_items SET product_id = ?, locked_price = ? WHERE id = ?", (replacement["id"], replacement["price"], item["id"]))
            try:
                reserve_ticket_stock(connection, ticket_id)
            except ValueError as exc:
                return redirect(start_response, f"/dashboard?message={str(exc)}")
            prior_block_id = ticket["delivery_block_id"]
            update_ticket(connection, ticket_id, status="PACKING", dispatcher_id=user["id"], review_reason=None, picker_id=None, driver_id=None, delivery_block_id=None, internal_note="Dispatcher updated products after review.")
            refresh_delivery_block_status(connection, prior_block_id)
            connection.commit()
            return redirect(start_response, "/dashboard?message=Ticket updated and returned to packing")
        return redirect(start_response, "/dashboard?message=That dispatch action is not allowed")

    if user["role"] == "driver":
        if ticket["driver_id"] != user["id"]:
            return redirect(start_response, "/dashboard?message=That route is not assigned to you")
        if action == "driver_emergency":
            emergency_type = data.get("emergency_type", "")
            meta = emergency_meta(emergency_type)
            connection.execute(
                """
                INSERT INTO support_tickets (
                    user_id, opened_by, category, priority, related_ticket_id, reason, status, resolution_note, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', '', ?, ?)
                """,
                (
                    user["id"],
                    user["id"],
                    f"EMERGENCY_{emergency_type.upper()}",
                    meta["priority"],
                    ticket_id,
                    meta["driver_message"],
                    now_iso(),
                    now_iso(),
                ),
            )
            connection.commit()
            return redirect(start_response, f"/dashboard?message={meta['driver_message']}")
        if action == "start_route" and ticket["status"] == "DRIVER_ASSIGNED":
            update_ticket(connection, ticket_id, status="OUT_FOR_DELIVERY")
            connection.commit()
            return redirect(start_response, "/dashboard?message=Route started")
        if action == "deliver_order" and ticket["status"] == "OUT_FOR_DELIVERY":
            if ticket["payment_status"] != "VERIFIED":
                return redirect(start_response, "/dashboard?message=Delivery cannot be completed until the bank verifies payment")
            update_ticket(connection, ticket_id, status="DELIVERED")
            connection.commit()
            return redirect(start_response, "/dashboard?message=Delivery completed")
        return redirect(start_response, "/dashboard?message=That driver action is not allowed")

    return redirect(start_response, "/dashboard?message=That action is not allowed")


def handle_update_user_account(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    target_id = int(data.get("user_id", "0"))
    target = connection.execute("SELECT * FROM users WHERE id = ?", (target_id,)).fetchone()
    if not target:
        return redirect(start_response, "/admin?message=Account not found")
    if target["role"] == "admin":
        return redirect(start_response, "/admin?message=Admin accounts cannot be changed here")
    account_state = data.get("account_state", "ACTIVE")
    reason = data.get("reason", "").strip()
    if not reason:
        return redirect(start_response, "/admin?message=Reason is required")
    connection.execute(
        "UPDATE users SET account_state = ?, account_reason = ? WHERE id = ?",
        (account_state, reason, target_id),
    )
    if account_state in {"LOCKED", "SUSPENDED", "BANNED"}:
        connection.execute(
            """
            INSERT INTO support_tickets (user_id, opened_by, category, reason, status, resolution_note, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'OPEN', '', ?, ?)
            """,
            (target_id, user["id"], account_state, reason, now_iso(), now_iso()),
        )
    log_activity(connection, user, "UPDATE_ACCOUNT_STATE", f"Set account state to {account_state} for {target['email']}. Reason: {reason}", target_user_id=target_id)
    connection.commit()
    message = "Account updated and support ticket created" if account_state in {"LOCKED", "SUSPENDED", "BANNED"} else "Account returned to active status"
    return redirect(start_response, f"/admin?message={message}")


def handle_update_support_ticket(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    ticket_id = int(data.get("ticket_id", "0"))
    status = data.get("status", "OPEN")
    resolution_note = data.get("resolution_note", "").strip()
    reply_message = data.get("reply_message", "").strip()
    assigned_to = data.get("assigned_to", "").strip()
    ticket = connection.execute("SELECT * FROM support_tickets WHERE id = ?", (ticket_id,)).fetchone()
    if not ticket:
        return redirect(start_response, "/admin?message=Support ticket not found")
    connection.execute(
        "UPDATE support_tickets SET status = ?, resolution_note = ?, assigned_to = ?, updated_at = ? WHERE id = ?",
        (status, resolution_note, int(assigned_to) if assigned_to else None, now_iso(), ticket_id),
    )
    if reply_message:
        connection.execute(
            "INSERT INTO support_messages (support_ticket_id, author_id, message, created_at) VALUES (?, ?, ?, ?)",
            (ticket_id, user["id"], reply_message, now_iso()),
        )
    log_activity(connection, user, "UPDATE_SUPPORT_TICKET", f"Updated support ticket #{ticket_id} to {status}.", target_user_id=ticket["user_id"])
    connection.commit()
    return redirect(start_response, "/admin?message=Support ticket updated")


def handle_update_guest_help(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    request_id = int(data.get("request_id", "0"))
    request_row = connection.execute("SELECT * FROM guest_help_requests WHERE id = ?", (request_id,)).fetchone()
    if not request_row:
        return redirect(start_response, "/admin?message=Registration help request not found")
    connection.execute(
        "UPDATE guest_help_requests SET status = ?, response_note = ?, updated_at = ? WHERE id = ?",
        (data.get("status", "OPEN"), data.get("response_note", "").strip(), now_iso(), request_id),
    )
    log_activity(connection, user, "UPDATE_GUEST_HELP", f"Updated registration help request #{request_id}.")
    connection.commit()
    return redirect(start_response, "/admin?message=Registration help request updated")


def handle_user_verification(environ, start_response, connection, user):
    gate = require_role(start_response, user, {"admin"})
    if gate:
        return gate
    data = read_post_data(environ)
    user_id = int(data.get("user_id", "0"))
    decision = data.get("decision", "")
    note = data.get("note", "").strip()
    target = connection.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    if not target:
        return redirect(start_response, "/admin?message=Account not found")
    if target["role"] != "client":
        return redirect(start_response, "/admin?message=Only customer accounts use ID verification")
    if decision == "approve":
        connection.execute(
            """
            UPDATE users
            SET account_state = 'ACTIVE',
                account_reason = NULL,
                verification_status = 'VERIFIED',
                verification_note = ?,
                verified_at = ?
            WHERE id = ?
            """,
            (note or "Verified by admin.", now_iso(), user_id),
        )
        log_activity(connection, user, "VERIFY_USER", f"Approved verification for {target['email']}.", target_user_id=user_id)
        connection.commit()
        return redirect(start_response, "/admin?message=Customer verification approved")
    if decision == "reject":
        if not note:
            note = "Verification rejected by admin."
        connection.execute(
            """
            UPDATE users
            SET account_state = 'LOCKED',
                account_reason = ?,
                verification_status = 'REJECTED',
                verification_note = ?,
                verified_at = NULL
            WHERE id = ?
            """,
            (note, note, user_id),
        )
        log_activity(connection, user, "REJECT_USER_VERIFICATION", f"Rejected verification for {target['email']}.", target_user_id=user_id)
        connection.commit()
        return redirect(start_response, "/admin?message=Customer verification rejected")
    return redirect(start_response, "/admin?message=Unknown verification action")


def serve_static(environ, start_response):
    file_path = os.path.join(STATIC_DIR, environ.get("PATH_INFO", "").replace("/static/", "", 1))
    if not os.path.isfile(file_path):
        return text_response(start_response, "Not found", status="404 Not Found", content_type="text/plain; charset=utf-8")
    with open(file_path, "rb") as handle:
        content = handle.read()
    content_type = "text/css; charset=utf-8" if file_path.endswith(".css") else "text/plain; charset=utf-8"
    start_response("200 OK", [("Content-Type", content_type)])
    return [content]


def application(environ, start_response):
    init_db()
    path = environ.get("PATH_INFO", "/")
    method = environ.get("REQUEST_METHOD", "GET").upper()
    if path.startswith("/static/"):
        return serve_static(environ, start_response)
    with db_connection() as connection:
        user = get_current_user(environ, connection)
        params = query_params(environ)
        message = params.get("message")
        if user and account_restricted(user) and path not in {"/dashboard", "/logout", "/help"}:
            return redirect(start_response, "/dashboard?message=Your account is restricted")
        if path == "/" and method == "GET":
            return text_response(start_response, render_store_page(connection, user=user, message=message, filters=params))
        if path == "/login":
            return handle_login(environ, start_response, connection) if method == "POST" else text_response(start_response, page("Login", login_form(), user=user))
        if path == "/register":
            return handle_register(environ, start_response, connection) if method == "POST" else text_response(start_response, page("Register", register_form(), user=user))
        if path == "/guest-help" and method == "POST":
            return handle_guest_help_request(environ, start_response, connection)
        if path == "/logout":
            destroy_session(environ, connection)
            return redirect(start_response, "/", f"{SESSION_COOKIE}=deleted; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
        if path == "/dashboard":
            gate = require_user(start_response, user)
            if gate:
                return gate
            if account_restricted(user):
                return text_response(start_response, restricted_account_page(user),)
            return text_response(start_response, render_dashboard(connection, user, message=message))
        if path == "/help":
            gate = require_user(start_response, user)
            return gate or text_response(start_response, render_helpdesk_dashboard(connection, user, message=message))
        if path == "/cart":
            gate = require_role(start_response, user, {"client"})
            return gate or text_response(start_response, render_cart_page(connection, user, message=message))
        if path == "/admin":
            gate = require_role(start_response, user, {"admin"})
            return gate or text_response(start_response, render_admin_dashboard(connection, user, message=message))
        if path == "/order" and method == "GET":
            gate = require_role(start_response, user, {"client"})
            if gate:
                return gate
            response = order_form(connection, int(query_params(environ).get("product_id", "0")), user)
            if response is None:
                return text_response(start_response, page("Order", "<section class='panel'><p>Menu item not found.</p></section>", user=user), status="404 Not Found")
            return text_response(start_response, response)
        if path == "/orders/create" and method == "POST":
            return handle_create_order(environ, start_response, connection, user)
        if path == "/orders/update" and method == "POST":
            return handle_update_order(environ, start_response, connection, user)
        if path == "/cart/add" and method == "POST":
            return handle_add_to_cart(environ, start_response, connection, user)
        if path == "/cart/remove" and method == "POST":
            return handle_remove_from_cart(environ, start_response, connection, user)
        if path == "/cart/checkout" and method == "POST":
            return handle_cart_checkout(environ, start_response, connection, user)
        if path == "/products/create" and method == "POST":
            return handle_create_product(environ, start_response, connection, user)
        if path == "/coupons/create" and method == "POST":
            return handle_create_coupon(environ, start_response, connection, user)
        if path == "/credits/issue" and method == "POST":
            return handle_issue_credit(environ, start_response, connection, user)
        if path == "/users/create" and method == "POST":
            return handle_create_user(environ, start_response, connection, user)
        if path == "/users/update" and method == "POST":
            return handle_update_user_account(environ, start_response, connection, user)
        if path == "/users/verify" and method == "POST":
            return handle_user_verification(environ, start_response, connection, user)
        if path == "/support/create" and method == "POST":
            return handle_create_support_ticket(environ, start_response, connection, user)
        if path == "/support/reply" and method == "POST":
            return handle_support_reply(environ, start_response, connection, user)
        if path == "/support/update" and method == "POST":
            return handle_update_support_ticket(environ, start_response, connection, user)
        if path == "/guest-help/update" and method == "POST":
            return handle_update_guest_help(environ, start_response, connection, user)
    return text_response(start_response, page("Not Found", "<section class='panel'><p>That page does not exist.</p></section>"), status="404 Not Found")


app = Flask(__name__, static_folder="static", static_url_path="/static")


@app.route("/", defaults={"path": ""}, methods=["GET", "POST"])
@app.route("/<path:path>", methods=["GET", "POST"])
def flask_routes(path):
    return Response.from_app(application, request.environ)


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
