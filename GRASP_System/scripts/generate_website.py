#!/usr/bin/env python3
"""
Generate static HTML genealogy website from MySQL database
Golden heritage theme with paper texture background
Version 5: Reads configuration from website_config.ini
"""

import mysql.connector
import os
from pathlib import Path
from datetime import datetime
from PIL import Image, ImageDraw, ImageFont
import shutil
import argparse
import configparser
import re

# ============================================================================
# NAME SORTING UTILITIES
# ============================================================================
# Standard sorting order: Surname, Given Name, Suffix
# Where suffix sorting is: no suffix first, then Jr/Sr, then Roman numerals (I, II, III, etc.)

def parse_suffix_for_sorting(suffix):
    """
    Parse a suffix and return a sortable tuple.
    
    Args:
        suffix: String suffix (e.g., "II", "Jr", "Sr", "III")
    
    Returns:
        Tuple of (category, order) where:
        - category: 0 for no suffix, 1 for Jr/Sr, 2 for Roman numerals
        - order: numeric order within category
    """
    if not suffix or suffix.strip() == '':
        return (0, 0)  # No suffix comes first
    
    suffix = suffix.strip().upper()
    
    # Handle Jr/Sr
    if suffix == 'JR' or suffix == 'JR.':
        return (1, 1)  # Jr comes before Sr
    if suffix == 'SR' or suffix == 'SR.':
        return (1, 2)
    
    # Handle Roman numerals
    roman_map = {
        'I': (2, 1),
        'II': (2, 2),
        'III': (2, 3),
        'IV': (2, 4),
        'V': (2, 5),
        'VI': (2, 6),
        'VII': (2, 7),
        'VIII': (2, 8),
        'IX': (2, 9),
        'X': (2, 10),
    }
    
    # Remove trailing period if present
    clean_suffix = suffix.rstrip('.')
    
    if clean_suffix in roman_map:
        return roman_map[clean_suffix]
    
    # Unknown suffix - sort after known suffixes
    return (3, suffix)


def create_name_sort_key(surname, given_name, suffix):
    """
    Create a sort key for a person's name following the standard sorting rules.
    
    Args:
        surname: Last name
        given_name: First and middle names
        suffix: Name suffix (Jr, Sr, II, III, etc.)
    
    Returns:
        Tuple that can be used as a sort key
    """
    # Normalize to handle None values
    # Treat empty surnames as "UNKNOWN" for sorting purposes
    surname_key = (surname or '').strip().upper()
    if not surname_key:
        surname_key = 'UNKNOWN'
    
    given_key = (given_name or '').strip().upper()
    if not given_key:
        given_key = 'UNKNOWN'
    
    suffix_key = parse_suffix_for_sorting(suffix)
    
    return (surname_key, given_key, suffix_key)


def sort_individuals_by_name(individuals):
    """
    Sort a list of individual dictionaries by name using standard rules.
    
    Args:
        individuals: List of dicts with 'surname', 'given_name', 'suffix' keys
    
    Returns:
        Sorted list of individuals
    """
    def get_sort_key(individual):
        surname = individual.get('surname', '')
        given = individual.get('given_name', '')
        suffix = individual.get('suffix', '')
        return create_name_sort_key(surname, given, suffix)
    
    return sorted(individuals, key=get_sort_key)

# ============================================================================
# END NAME SORTING UTILITIES
# ============================================================================

def get_source_link_filename(source_id, cursor=None, individual_id=None):
    """
    Convert a source_id to the filename format used for source detail pages.
    Handles both single-page and dual-page (sorted) sources intelligently.
    
    Args:
        source_id: Source ID like @S177@
        cursor: Database cursor (optional). If provided, will check if source has family events
        individual_id: Individual ID (optional). If provided with cursor, will determine
                      if this person is husband or wife in family events and link to appropriate version
    
    Returns:
        Cleaned filename like 's177', 's177_by_husband', or 's177_by_wife' (without .html extension)
    """
    clean_id = source_id.replace('@', '').replace('S', 's')
    
    # If no cursor provided, just return the basic filename
    if cursor is None:
        return clean_id
    
    # Check if this source has any family events
    cursor.execute("""
        SELECT COUNT(*) as count
        FROM event_source_xref esx
        JOIN events e ON esx.event_id = e.event_id
        JOIN fam_event_xref fex ON e.event_id = fex.event_id
        WHERE esx.source_id = %s
    """, (source_id,))
    result = cursor.fetchone()
    
    # If no family events, use simple filename
    if not result or result['count'] == 0:
        return clean_id
    
    # Source has family events - need to determine which version to link to
    # If individual_id provided, check if they're husband or wife
    if individual_id:
        cursor.execute("""
            SELECT f.husband_id, f.wife_id
            FROM event_source_xref esx
            JOIN events e ON esx.event_id = e.event_id
            JOIN fam_event_xref fex ON e.event_id = fex.event_id
            JOIN families f ON fex.family_id = f.family_id
            WHERE esx.source_id = %s
            LIMIT 1
        """, (source_id,))
        family = cursor.fetchone()
        
        if family:
            if family['wife_id'] == individual_id:
                return f"{clean_id}_by_wife"
            # Default to husband for husband or when person not found in family
            return f"{clean_id}_by_husband"
    
    # No individual context - default to husband-sorted version
    return f"{clean_id}_by_husband"

def convert_urls_to_links(text, show_url=True):
    """
    Convert URLs in text to clickable HTML links.
    
    Args:
        text: Text containing URLs
        show_url: If True, shows the URL as the link text. If False, shows "View online source"
    
    Detects URLs starting with http://, https://, or www.
    """
    if not text:
        return text
    
    # Pattern to match URLs
    url_pattern = r'(https?://[^\s]+|www\.[^\s]+)'
    
    def replace_url(match):
        url = match.group(1)
        # Add http:// if URL starts with www.
        full_url = url if url.startswith('http') else f'http://{url}'
        
        if show_url:
            # Show the URL as the link text
            return f'<a href="{full_url}" target="_blank">{url}</a>'
        else:
            # Show "View online source" as link text
            return f'<a href="{full_url}" target="_blank">View online source</a>'
    
    return re.sub(url_pattern, replace_url, text)

def clean_publication_info(pub_info):
    """Remove runs of bare semicolons and blank segments from publication_info strings.
    Splits on semicolons, strips each segment, discards empty ones, rejoins."""
    if not pub_info:
        return ''
    segments = [s.strip() for s in pub_info.split(';')]
    segments = [s for s in segments if s]
    return '; '.join(segments)


def parse_gedcom_date(date_str):
    """
    Parse GEDCOM date string and return a sortable tuple (year, month, day, modifier_penalty)
    
    GEDCOM dates can have formats like:
    - "25 DEC 1950"
    - "DEC 1950"
    - "1950"
    - "BEF 1950"
    - "AFT 25 DEC 1950"
    - "ABT 1950"
    - "BET 1940 AND 1950"
    
    Returns tuple: (year, month, day, penalty)
    - penalty is used to sort modified dates (BEF, AFT, ABT) appropriately
    - Returns (9999, 99, 99, 99) for invalid/empty dates (sorts last)
    """
    if not date_str or date_str.strip() == '':
        return (9999, 99, 99, 99)
    
    date_str = date_str.strip().upper()
    
    # Month name to number mapping
    months = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
    }
    
    # Default values
    year = 9999
    month = 99
    day = 99
    penalty = 50  # Default penalty for exact dates
    
    # Handle date modifiers
    if date_str.startswith('BEF '):
        date_str = date_str[4:]
        penalty = 40  # Sort before exact date
    elif date_str.startswith('AFT '):
        date_str = date_str[4:]
        penalty = 60  # Sort after exact date
    elif date_str.startswith('ABT ') or date_str.startswith('EST ') or date_str.startswith('CAL '):
        date_str = date_str[4:]
        penalty = 50  # Sort as exact date
    elif date_str.startswith('BET '):
        # For "BET year1 AND year2", use the first year
        match = re.search(r'BET\s+(\d+)', date_str)
        if match:
            year = int(match.group(1))
            penalty = 45
        return (year, month, day, penalty)
    elif date_str.startswith('FROM '):
        date_str = date_str[5:]
        penalty = 50
    
    # Try to parse the date components
    parts = date_str.split()
    
    try:
        if len(parts) == 3:
            # Format: "25 DEC 1950"
            day = int(parts[0])
            month = months.get(parts[1], 99)
            year = int(parts[2])
        elif len(parts) == 2:
            # Format: "DEC 1950" or "25 1950"
            if parts[0] in months:
                month = months[parts[0]]
                year = int(parts[1])
            elif parts[0].isdigit() and len(parts[0]) <= 2:
                day = int(parts[0])
                year = int(parts[1])
            else:
                year = int(parts[1])
        elif len(parts) == 1:
            # Format: "1950"
            if parts[0].isdigit():
                year = int(parts[0])
    except (ValueError, IndexError):
        # If parsing fails, return default (sorts last)
        pass
    
    return (year, month, day, penalty)

# Database configuration -- populated by load_configuration() from website_config.ini
DB_CONFIG = {}

# Output directory
OUTPUT_DIR = Path('website')

# Configuration file path (may be overridden by --config argument)
CONFIG_FILE = Path('website_config.ini')

# Global configuration dictionary
SITE_CONFIG = {}

def load_configuration():
    """Load website configuration from INI file"""
    global SITE_CONFIG, DB_CONFIG

    if not CONFIG_FILE.exists():
        print(f"ERROR: Configuration file not found: {CONFIG_FILE}")
        print("Please create website_config.ini in the genealogy folder.")
        exit(1)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    # Load database credentials
    if not config.has_section('Database'):
        print("ERROR: website_config.ini is missing the [Database] section.")
        print("Please add Host, User, Password, and Database under [Database].")
        exit(1)
    DB_CONFIG['host']     = config.get('Database', 'Host',     fallback='localhost')
    DB_CONFIG['user']     = config.get('Database', 'User',     fallback='root')
    DB_CONFIG['password'] = config.get('Database', 'Password', fallback='')
    DB_CONFIG['database'] = config.get('Database', 'Database', fallback='genealogy')

    # Load website settings
    SITE_CONFIG['header_name'] = config.get('Website', 'Header_Name', fallback='Family Genealogy')
    SITE_CONFIG['logo_file'] = config.get('Website', 'Logo_File', fallback='logo.gif')
    SITE_CONFIG['assets_dir'] = config.get('Website', 'Assets_Dir', fallback='assets')
    SITE_CONFIG['copyright_info'] = config.get('Website', 'Copyright_Info', fallback='All rights reserved.')
    SITE_CONFIG['welcome_family'] = config.get('Website', 'Welcome_Family', fallback='Family Genealogy')
    SITE_CONFIG['css_file'] = config.get('Website', 'CSS_File', fallback='styles.css')
    SITE_CONFIG['styles_source'] = config.get('Paths', 'Styles_Source', fallback='')

    # Load contact settings
    SITE_CONFIG['contact'] = config.get('Contact', 'Contact', fallback='')
    SITE_CONFIG['email'] = config.get('Contact', 'Email', fallback='')

    print("\nConfiguration loaded:")
    print(f"  Database: {DB_CONFIG['database']} on {DB_CONFIG['host']} (user: {DB_CONFIG['user']})")
    print(f"  Header: {SITE_CONFIG['header_name']}")
    print(f"  Logo: {SITE_CONFIG['logo_file']}")
    print(f"  CSS: {SITE_CONFIG['css_file']}")
    print(f"  Contact: {SITE_CONFIG['contact']}")
    print(f"  Email: {SITE_CONFIG['email']}")

def copy_assets():
    """Copy logo, CSS, and template files from assets folder to website folder"""
    print("\nCopying website assets...")
    
    source_dir = Path(SITE_CONFIG.get('assets_dir', 'assets'))
    
    # Copy logo file
    logo_filename = Path(SITE_CONFIG['logo_file']).name
    logo_source = source_dir / logo_filename
    logo_dest = OUTPUT_DIR / 'images' / logo_filename
    
    if logo_source.exists():
        shutil.copy2(logo_source, logo_dest)
        print(f"  Copied logo: {SITE_CONFIG['logo_file']}")
    else:
        print(f"  WARNING: Logo file not found: {logo_source}")
    
    # Copy CSS file — check Styles_Source, then assets/, then generate default
    css_dest = OUTPUT_DIR / 'css' / 'style.css'
    styles_source_dir = SITE_CONFIG.get('styles_source', '').strip()
    css_found = False

    if styles_source_dir:
        css_try = Path(styles_source_dir) / SITE_CONFIG['css_file']
        if css_try.exists():
            shutil.copy2(css_try, css_dest)
            print(f"  Copied CSS: {SITE_CONFIG['css_file']} -> style.css")
            css_found = True

    if not css_found:
        css_try = source_dir / SITE_CONFIG['css_file']
        if css_try.exists():
            shutil.copy2(css_try, css_dest)
            print(f"  Copied CSS: {SITE_CONFIG['css_file']} -> style.css")
            css_found = True

    if not css_found:
        print(f"  WARNING: CSS file not found: {SITE_CONFIG['css_file']}")
        print("  Generating default CSS instead...")
        generate_css()
    
    # Copy Template.jpg if it exists
    template_source = source_dir / 'Template.jpg'
    template_dest = OUTPUT_DIR / 'images' / 'Template.jpg'
    
    if template_source.exists():
        shutil.copy2(template_source, template_dest)
        print(f"  Copied template: Template.jpg")
    else:
        print(f"  Note: Template.jpg not found (optional)")

    # Copy background1.jpg if it exists (used as tiling body background in CSS)
    bg_source = source_dir / 'background1.jpg'
    bg_dest = OUTPUT_DIR / 'images' / 'background1.jpg'
    if bg_source.exists():
        shutil.copy2(bg_source, bg_dest)
        print(f"  Copied background: background1.jpg -> images/background1.jpg")
    else:
        print(f"  Note: background1.jpg not found (optional)")


def format_event_type(event_type):
    """Convert GEDCOM event codes to readable labels"""
    event_labels = {
        'NAME': 'Name',
        'BIRT': 'Birth',
        'CHR': 'Christening',
        'DEAT': 'Death',
        'BURI': 'Burial',
        'CREM': 'Cremation',
        'ADOP': 'Adoption',
        'BAPM': 'Baptism',
        'BARM': 'Bar Mitzvah',
        'BASM': 'Bas Mitzvah',
        'BLES': 'Blessing',
        'CHRA': 'Adult Christening',
        'CONF': 'Confirmation',
        'FCOM': 'First Communion',
        'ORDN': 'Ordination',
        'NATU': 'Naturalization',
        'EMIG': 'Emigration',
        'IMMI': 'Immigration',
        'CENS': 'Census',
        'PROB': 'Probate',
        'WILL': 'Will',
        'GRAD': 'Graduation',
        'RETI': 'Retirement',
        'EVEN': 'Event',
        'CAST': 'Caste',
        'DSCR': 'Physical Description',
        'EDUC': 'Education',
        'IDNO': 'National ID Number',
        'NATI': 'Nationality',
        'NCHI': 'Number of Children',
        'NMR': 'Number of Marriages',
        'OCCU': 'Occupation',
        'PROP': 'Property',
        'RELI': 'Religion',
        'RESI': 'Residence',
        'SSN': 'Social Security Number',
        'TITL': 'Title',
        'FACT': 'Fact',
        'MARR': 'Marriage',
        'MARB': 'Marriage Banns',
        'MARC': 'Marriage Contract',
        'MARL': 'Marriage License',
        'MARS': 'Marriage Settlement',
        'DIV': 'Divorce',
        'DIVF': 'Divorce Filed',
        'ENGA': 'Engagement',
        'ANUL': 'Annulment'
    }
    
    # Return formatted label or the original code if not found
    return event_labels.get(event_type, event_type)

def get_db_connection():
    """Create database connection"""
    return mysql.connector.connect(**DB_CONFIG)

def clean_old_website():
    """
    Remove old HTML and CSS files from website directory before generating new ones.
    Preserves the images and thumbnails directories.
    """
    print("\nCleaning old website files...")
    
    if not OUTPUT_DIR.exists():
        print("  No existing website directory to clean")
        return
    
    files_removed = 0
    dirs_removed = 0
    
    # Remove all .html files from root directory
    for html_file in OUTPUT_DIR.glob('*.html'):
        html_file.unlink()
        files_removed += 1
    
    # Remove entire css directory (will be recreated)
    css_dir = OUTPUT_DIR / 'css'
    if css_dir.exists():
        shutil.rmtree(css_dir)
        dirs_removed += 1
    
    # Remove HTML files from subdirectories (but keep the directories)
    for subdir in ['individuals', 'families', 'events', 'places', 'sources', 'media', 'repositories', 'notes']:
        subdir_path = OUTPUT_DIR / subdir
        if subdir_path.exists():
            for html_file in subdir_path.glob('*.html'):
                html_file.unlink()
                files_removed += 1
    
    print(f"  Removed {files_removed} HTML files and {dirs_removed} directories")
    print("  Preserved images/ and thumbnails/ directories")


def create_directories():
    """Create necessary directories for the website"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / 'css').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'images').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'thumbnails').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'individuals').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'families').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'events').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'places').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'sources').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'media').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'notes').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'repositories').mkdir(exist_ok=True)
    (OUTPUT_DIR / 'queries').mkdir(exist_ok=True)

def get_document_type_color(extension):
    """Get the appropriate color for a document type"""
    colors = {
        'pdf': (220, 53, 69),      # Red
        'doc': (41, 98, 255),      # Blue
        'docx': (41, 98, 255),     # Blue
        'html': (255, 127, 0),     # Orange
        'htm': (255, 127, 0),      # Orange
        'txt': (108, 117, 125),    # Gray
        'xls': (34, 139, 34),      # Green
        'xlsx': (34, 139, 34),     # Green
        'ppt': (211, 84, 0),       # Dark Orange
        'pptx': (211, 84, 0),      # Dark Orange
        'rtf': (102, 102, 153),    # Purple-gray
    }
    return colors.get(extension.lower(), (128, 128, 128))  # Default gray

def create_document_icon(output_path, doc_type, bg_color, text_color=(255, 255, 255), size=(150, 150)):
    """
    Create a simple document icon with file type label
    
    Args:
        output_path: Where to save the icon
        doc_type: Type label (e.g., 'PDF', 'HTML', 'DOCX')
        bg_color: Background color as RGB tuple
        text_color: Text color as RGB tuple
        size: Icon size as tuple (width, height)
    """
    # Create image with background color
    img = Image.new('RGB', size, bg_color)
    draw = ImageDraw.Draw(img)
    
    # Draw document shape (folded corner)
    margin = 20
    fold_size = 25
    
    # Main document rectangle
    doc_rect = [margin, margin, size[0] - margin, size[1] - margin]
    
    # Create document shape with folded corner
    points = [
        (doc_rect[0], doc_rect[1]),  # Top left
        (doc_rect[2] - fold_size, doc_rect[1]),  # Top right (before fold)
        (doc_rect[2], doc_rect[1] + fold_size),  # Folded corner
        (doc_rect[2], doc_rect[3]),  # Bottom right
        (doc_rect[0], doc_rect[3]),  # Bottom left
        (doc_rect[0], doc_rect[1])   # Back to top left
    ]
    
    # Draw document outline
    draw.polygon(points, fill=(255, 255, 255), outline=text_color, width=3)
    
    # Draw fold lines
    fold_points = [
        (doc_rect[2] - fold_size, doc_rect[1]),
        (doc_rect[2] - fold_size, doc_rect[1] + fold_size),
        (doc_rect[2], doc_rect[1] + fold_size)
    ]
    draw.line(fold_points, fill=text_color, width=2)
    
    # Add document type text
    try:
        font_size = 32 if len(doc_type) <= 4 else 24
        try:
            # Mac fonts
            font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", font_size)
        except:
            try:
                # Windows fonts
                font = ImageFont.truetype("arial.ttf", font_size)
            except:
                try:
                    # Linux fonts
                    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", font_size)
                except:
                    font = ImageFont.load_default()
    except:
        font = ImageFont.load_default()
    
    # Get text bounding box and center it
    bbox = draw.textbbox((0, 0), doc_type, font=font)
    text_width = bbox[2] - bbox[0]
    text_height = bbox[3] - bbox[1]
    text_x = (size[0] - text_width) // 2
    text_y = (size[1] - text_height) // 2
    
    # Draw text
    draw.text((text_x, text_y), doc_type, fill=bg_color, font=font)
    
    # Save the icon
    img.save(output_path, 'JPEG', quality=95)
    return True

def generate_thumbnail(source_path, thumb_path, size=(150, 150)):
    """Generate a thumbnail from an image file or create a document icon for non-images"""
    source_path = Path(source_path)
    extension = source_path.suffix.lower().replace('.', '')
    
    # Check if it's a document type that needs an icon
    document_types = ['pdf', 'doc', 'docx', 'html', 'htm', 'txt', 'xls', 'xlsx', 'ppt', 'pptx', 'rtf']
    
    if extension in document_types:
        # Create a document icon instead of trying to thumbnail
        try:
            bg_color = get_document_type_color(extension)
            create_document_icon(thumb_path, extension.upper(), bg_color, size=size)
            return True
        except Exception as e:
            # Silently fail for document icons - not critical
            return False
    
    # For image files, generate normal thumbnail
    try:
        with Image.open(source_path) as img:
            # Convert to RGB if necessary (for PNG with transparency, etc.)
            if img.mode in ('RGBA', 'LA', 'P'):
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                img = background
            
            # Generate thumbnail maintaining aspect ratio
            img.thumbnail(size, Image.Resampling.LANCZOS)
            img.save(thumb_path, 'JPEG', quality=85)
            return True
    except Exception as e:
        # If it's not an image we can process, try creating a generic document icon
        # Only for known extensions - don't warn about these
        if extension:
            try:
                bg_color = get_document_type_color(extension)
                create_document_icon(thumb_path, extension.upper(), bg_color, size=size)
                return True
            except:
                pass
        # Only print warning for actual failures on image files
        return False

def process_media_files():
    """Process all media files: copy originals and generate thumbnails"""
    print("\nProcessing media files...")
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT media_id, file_path, format FROM media WHERE file_path IS NOT NULL")
    media_files = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    if not media_files:
        print("  No media files found in database")
        return
    
    # Base directory where source media files are located
    source_base = Path.cwd()  # Current directory
    
    copied_count = 0
    thumbnail_count = 0
    icon_count = 0
    missing_files = []
    
    # Document types that will get icons
    document_types = ['pdf', 'doc', 'docx', 'html', 'htm', 'txt', 'xls', 'xlsx', 'ppt', 'pptx', 'rtf']
    
    for media in media_files:
        file_path = media['file_path']
        if not file_path:
            continue
        
        # Source file path (from your current location)
        source_file = source_base / file_path
        
        # Get just the filename
        filename = Path(file_path).name
        extension = Path(filename).suffix.lower().replace('.', '')
        
        # Destination paths in website
        dest_file = OUTPUT_DIR / 'images' / filename
        thumb_file = OUTPUT_DIR / 'thumbnails' / f"{Path(filename).stem}_thumb.jpg"
        
        # Try to find the actual file - handle double extensions
        actual_source = None
        if source_file.exists():
            actual_source = source_file
        else:
            # Check for double extension (e.g., IMG_6896.PNG.jpg -> IMG_6896.PNG)
            stem_path = Path(source_file.stem)
            if stem_path.suffix:
                # Try without the last extension
                alternate_source = source_file.parent / source_file.stem
                if alternate_source.exists():
                    actual_source = alternate_source
                    # Update filename to match actual file
                    filename = alternate_source.name
                    dest_file = OUTPUT_DIR / 'images' / filename
        
        # Copy original file if we found it
        if actual_source:
            try:
                shutil.copy2(actual_source, dest_file)
                copied_count += 1
                
                # Generate thumbnail or icon
                if generate_thumbnail(actual_source, thumb_file):
                    thumbnail_count += 1
                    # Check if we created a document icon
                    actual_ext = Path(actual_source).suffix.lower().replace('.', '')
                    if actual_ext in document_types:
                        icon_count += 1
                        
            except Exception as e:
                print(f"  Warning: Could not process {file_path}: {e}")
        else:
            # File not found - create placeholder icon if it's a document type
            if extension in document_types:
                try:
                    bg_color = get_document_type_color(extension)
                    create_document_icon(thumb_file, extension.upper(), bg_color)
                    thumbnail_count += 1
                    icon_count += 1
                    # Track as missing but don't print immediately
                    missing_files.append(file_path)
                except:
                    missing_files.append(file_path)
            else:
                # Only track non-document missing files for summary
                missing_files.append(file_path)
    
    print(f"  Copied {copied_count} media files")
    print(f"  Generated {thumbnail_count} thumbnails ({icon_count} document icons)")
    
    # Summarize missing files at the end instead of individual warnings
    if missing_files:
        print(f"  Note: {len(missing_files)} source files not found (placeholder icons created where applicable)")
        # Optionally show first few examples
        if len(missing_files) <= 10:
            for mf in missing_files[:5]:
                print(f"    - {Path(mf).name}")
            if len(missing_files) > 5:
                print(f"    ... and {len(missing_files) - 5} more")


def generate_css():
    """Fallback CSS generator — only called if no CSS file is found in config paths.
    The embedded CSS was removed; place a styles.css in your Styles_Source directory."""
    print("  ERROR: No CSS file found and no fallback CSS is available.")
    print("  Please set Styles_Source in config.ini and ensure your CSS file exists.")

def get_html_header(title, depth=0):
    """Generate HTML header with navigation
    
    Args:
        title: Page title
        depth: Directory depth (0 for root, 1 for subdirectories, etc.)
    """
    # Navigation items
    nav_items = [
        ('index.html', 'Home'),
        ('individuals/index.html', 'Individuals'),
        ('families/index.html', 'Families'),
        ('events/index.html', 'Events'),
        ('places/index.html', 'Places'),
        ('sources/index.html', 'Sources'),
        ('media/index.html', 'Media'),
        ('notes/index.html', 'Notes'),
        ('repositories/index.html', 'Repositories'),
        ('queries.html', 'Queries')
    ]
    
    # Adjust paths based on depth
    prefix = '../' * depth if depth > 0 else ''
    
    nav_html = '\n'.join([
        f'<a href="{prefix}{url}">{label}</a>'
        for url, label in nav_items
    ])
    
    # Get logo filename from config
    logo_file = SITE_CONFIG.get('logo_file', 'logo.gif')
    header_name = SITE_CONFIG.get('header_name', 'Family Genealogy')
    
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title} - {header_name}</title>
    <link rel="stylesheet" href="{prefix}css/style.css">
</head>
<body>
    <div class="container">
        <header>
            <img src="{prefix}images/{logo_file}" alt="{header_name} Logo" class="logo">
            <h1>{header_name}</h1>
            <p>A Heritage of Generations</p>
        </header>
        <nav>
            {nav_html}
        </nav>
        <div class="content">
"""

def get_html_footer():
    """Generate HTML footer with contact information and copyright"""
    current_year = datetime.now().year
    
    # Get contact info from config
    contact = SITE_CONFIG.get('contact', '')
    email = SITE_CONFIG.get('email', '')
    copyright_info = SITE_CONFIG.get('copyright_info', 'All rights reserved.')
    
    # Build contact section
    contact_html = ""
    if contact or email:
        contact_html = "<p>"
        if contact:
            # Split contact by "/" and add line breaks
            contact_lines = contact.split('/')
            contact_html += "<br>".join(contact_lines)
        if email:
            if contact:
                contact_html += "<br>"
            contact_html += f'<a href="mailto:{email}">{email}</a>'
        contact_html += "</p>"
    
    return f"""
        </div>
        <div class="footer">
            <p>Generated on {datetime.now().strftime('%B %d, %Y')}</p>
            {contact_html}
            <p class="copyright">&copy; {current_year} {copyright_info}</p>
        </div>
    </div>
</body>
</html>
"""

def generate_index():
    """Generate the main index page"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) as count FROM individuals")
    ind_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM families")
    fam_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM events")
    evt_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM places")
    place_count = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(*) as count FROM sources")
    source_count = cursor.fetchone()['count']
    
    # Get welcome message from config
    welcome_family = SITE_CONFIG.get('welcome_family', 'Family Genealogy')
    
    html = get_html_header('Welcome', 0)
    html += f"""
        <div class="welcome">
            <h2>Welcome to the {welcome_family}</h2>
            <p style="font-size: 1.2em; line-height: 1.6; margin: 20px 0;">
            This website contains the comprehensive genealogical records of the {welcome_family.replace(' Genealogy', '').replace(' Family', '')} family, 
            preserving our heritage for current and future generations.
            </p>
        </div>
        
        <div class="stats-template-container">
            <div class="info-section">
                <h3>Database Statistics</h3>
                <ul style="font-size: 1.1em;">
"""
    html += f"""
                <li><strong>{ind_count}</strong> Individuals</li>
                <li><strong>{fam_count}</strong> Families</li>
                <li><strong>{evt_count}</strong> Events</li>
                <li><strong>{place_count}</strong> Places</li>
                <li><strong>{source_count}</strong> Sources</li>
"""
    html += """
            </ul>
            </div>
            
            <div class="template-image">
                <img src="images/Template.jpg" alt="Genealogy Symbol Legend" style="max-width: 100%; height: auto; border: 2px solid #8B4513; border-radius: 5px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);">
            </div>
        </div>
        
        <div class="info-section">
            <h3>Explore Our Heritage</h3>
            <p>Use the navigation menu above to browse through different sections:</p>
            <ul style="font-size: 1.1em;">
                <li><strong>Individuals:</strong> Browse all family members alphabetically</li>
                <li><strong>Families:</strong> View family units and relationships</li>
                <li><strong>Events:</strong> Timeline of births, marriages, deaths, and other events</li>
                <li><strong>Places:</strong> Locations significant to our family history</li>
                <li><strong>Sources:</strong> Documentation and references</li>
                <li><strong>Media:</strong> Photos, documents, and other media</li>
                <li><strong>Notes:</strong> Additional information and stories</li>
                <li><strong>Repositories:</strong> Archives and collections</li>
            </ul>
        </div>
    """
    
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print("Generated index.html")

def generate_individuals_index():
    """Generate the individuals index page with table format and alphabetical navigation"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Get all individuals (we'll sort in Python using our standard sort)
    cursor.execute("""
        SELECT individual_id, given_name, surname, suffix, sex, birth_date, death_date
        FROM individuals
    """)
    individuals = cursor.fetchall()
    
    # Sort using standardized name sorting: surname, suffix, given name
    individuals = sort_individuals_by_name(individuals)
    
    # Build alphabetical index
    letters = set()
    for ind in individuals:
        surname = ind['surname'] or ''
        if surname:
            first_letter = surname[0].upper()
            if first_letter.isalpha():
                letters.add(first_letter)
    
    letters = sorted(letters)
    
    html = get_html_header('Individuals', 1)
    html += f"""
        <div id="top"></div>
        <h2>Individuals ({len(individuals)})</h2>
        <p>Browse all individuals in the database, sorted alphabetically by surname.</p>
"""
    
    # Add alphabetical navigation
    if letters:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Name</th>
                    <th>ID</th>
                    <th>Sex</th>
                    <th>Birth</th>
                    <th>Death</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_letter = None
    for ind in individuals:
        # Build full name with suffix
        given = ind['given_name'] or ''
        surname = ind['surname'] or ''
        suffix = ind['suffix'] or ''
        
        name_parts = [given, surname]
        if suffix:
            name_parts.append(suffix)
        name = ' '.join(filter(None, name_parts)) or 'Unknown'
        
        sex = ind['sex'] or ''
        birth = ind['birth_date'] or ''
        death = ind['death_date'] or ''
        
        # Add letter anchor when surname first letter changes
        if surname:
            first_letter = surname[0].upper()
            if first_letter.isalpha() and first_letter != current_letter:
                current_letter = first_letter
                html += f"""
                <tr class="letter-divider" id="letter-{current_letter}">
                    <td>{current_letter}</td>
                    <td colspan="4" style="text-align: right;">
                        <a href="#top">RETURN TO TOP</a>
                    </td>
                </tr>
"""
        
        html += f"""
                <tr>
                    <td><a href="individual_{ind['individual_id']}.html">{name}</a></td>
                    <td><span class="cell-text">{ind['individual_id']}</span></td>
                    <td><span class="cell-text">{sex}</span></td>
                    <td><span class="cell-text">{birth}</span></td>
                    <td><span class="cell-text">{death}</span></td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'individuals' / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated individuals/index.html with {len(individuals)} individuals")

def generate_individual_pages():
    """Generate individual detail pages"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT individual_id, given_name, surname, suffix, sex FROM individuals")
    individuals = cursor.fetchall()
    
    for ind in individuals:
        individual_id = ind['individual_id']
        
        # Build full name with suffix
        given = ind['given_name'] or ''
        surname = ind['surname'] or ''
        suffix = ind['suffix'] or ''
        
        name_parts = [given, surname]
        if suffix:
            name_parts.append(suffix)
        name = ' '.join(filter(None, name_parts)) or 'Unknown'
        
        # Get all events for this individual
        cursor.execute("""
            SELECT e.event_id, e.event_type, e.event_date, e.event_place, e.event_value, e.full_name, NULL as family_id
            FROM events e
            JOIN indi_event_xref ix ON e.event_id = ix.event_id
            WHERE ix.individual_id = %s
        """, (individual_id,))
        events = list(cursor.fetchall())
        
        # Also get family events (MARR, DIV, ENGA, ANUL) for this individual
        cursor.execute("""
            SELECT e.event_id, e.event_type, e.event_date, e.event_place, e.event_value, NULL as full_name, fex.family_id,
                   CASE 
                       WHEN f.husband_id = %s THEN w.individual_id
                       ELSE h.individual_id
                   END as spouse_id,
                   CASE 
                       WHEN f.husband_id = %s THEN w.given_name
                       ELSE h.given_name
                   END as spouse_given,
                   CASE 
                       WHEN f.husband_id = %s THEN w.surname
                       ELSE h.surname
                   END as spouse_surname,
                   CASE 
                       WHEN f.husband_id = %s THEN w.suffix
                       ELSE h.suffix
                   END as spouse_suffix
            FROM events e
            JOIN fam_event_xref fex ON e.event_id = fex.event_id
            JOIN families f ON fex.family_id = f.family_id
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE (f.husband_id = %s OR f.wife_id = %s) AND e.event_type IN ('MARR', 'DIV', 'ENGA', 'ANUL')
        """, (individual_id, individual_id, individual_id, individual_id, individual_id, individual_id))
        family_events = cursor.fetchall()
        events.extend(family_events)
        
        # Sort events by parsed date (chronologically with undated events last)
        events.sort(key=lambda evt: parse_gedcom_date(evt['event_date']))
        
        # Get families where this person is a child
        cursor.execute("""
            SELECT f.family_id, 
                   f.husband_id as father_id, h.given_name as father_given, h.surname as father_surname, h.suffix as father_suffix,
                   f.wife_id as mother_id, w.given_name as mother_given, w.surname as mother_surname, w.suffix as mother_suffix
            FROM child_family_xref cf
            JOIN families f ON cf.family_id = f.family_id
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE cf.child_id = %s
        """, (individual_id,))
        child_families = cursor.fetchall()
        
        # Get families where this person is a spouse
        cursor.execute("""
            SELECT f.family_id, f.marriage_date,
                   CASE 
                       WHEN f.husband_id = %s THEN w.given_name
                       ELSE h.given_name
                   END as spouse_given,
                   CASE 
                       WHEN f.husband_id = %s THEN w.surname
                       ELSE h.surname
                   END as spouse_surname,
                   CASE 
                       WHEN f.husband_id = %s THEN w.suffix
                       ELSE h.suffix
                   END as spouse_suffix,
                   CASE 
                       WHEN f.husband_id = %s THEN f.wife_id
                       ELSE f.husband_id
                   END as spouse_id
            FROM families f
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE f.husband_id = %s OR f.wife_id = %s
        """, (individual_id, individual_id, individual_id, individual_id, individual_id, individual_id))
        spouse_families = cursor.fetchall()
        
        # Get media for this individual
        cursor.execute("""
            SELECT m.media_id, m.file_path, m.title
            FROM media m
            JOIN indi_media_xref im ON m.media_id = im.media_id
            WHERE im.individual_id = %s
        """, (individual_id,))
        media = cursor.fetchall()
        
        # Get _PHOTO custom tag for profile photo
        cursor.execute("""
            SELECT tag_value
            FROM custom_tags
            WHERE parent_type = 'INDI' 
            AND parent_id = %s 
            AND tag_name = '_PHOTO'
            LIMIT 1
        """, (individual_id,))
        photo_result = cursor.fetchone()
        
        # Initialize set to track displayed media IDs
        displayed_media_ids = set()
        
        # Find the photo media file if _PHOTO tag exists
        photo_path = None
        photo_media_id = None
        if photo_result and photo_result['tag_value']:
            photo_media_id = photo_result['tag_value']
            # Track this media as displayed
            displayed_media_ids.add(photo_media_id)
            
            # Look up the media file path
            cursor.execute("""
                SELECT file_path FROM media WHERE media_id = %s
            """, (photo_media_id,))
            photo_media = cursor.fetchone()
            if photo_media and photo_media['file_path']:
                from pathlib import Path
                filename = Path(photo_media['file_path']).name
                photo_path = f"../images/{filename}"
        
        # Generate HTML
        html = get_html_header(name, 1)
        html += """
        <div>
            <a href="index.html" class="return-to-index">← RETURN TO INDIVIDUALS INDEX</a>
        </div>
"""
        html += f"""
        <h2>Individual: {name}</h2> 
        <div class="detail-box-individual" style="position: relative;">
"""
        
        # Add profile photo if available - floats on right across entire box
        if photo_path:
            html += f"""
            <div style="float: right; margin-left: 20px; margin-bottom: 10px;">
                <img src="{photo_path}" alt="{name}" style="max-width: 200px; max-height: 125px; border: 3px solid #D4AF37; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.2);">
            </div>
"""
        
        # ID & Sex
        html += f"""
            <p><strong>ID:</strong> <span class="cell-text">{individual_id}</span></p>
"""

        # Families line — all family IDs this person belongs to (as child or spouse)
        all_family_ids = []
        for fam in child_families:
            if fam['family_id'] not in all_family_ids:
                all_family_ids.append(fam['family_id'])
        for fam in spouse_families:
            if fam['family_id'] not in all_family_ids:
                all_family_ids.append(fam['family_id'])

        if all_family_ids:
            badge_style = 'class="badge-link"'
            family_links = ' '.join(
                f'<a href="../families/family_{fid}.html" {badge_style}>{fid}</a>'
                for fid in all_family_ids
            )
            html += f"""
            <p><strong>Families:</strong> {family_links}</p>
"""

        html += f"""
            <p><strong>Sex:</strong> {ind['sex'] or 'Unknown'}</p>
"""

        # Parents section (inside the same detail-box)
        if child_families:
            for fam in child_families:
                # Build father name and link
                father_parts = [fam['father_given'] or '', fam['father_surname'] or '']
                if fam.get('father_suffix'):
                    father_parts.append(fam['father_suffix'])
                father_name = ' '.join(filter(None, father_parts)) or 'Unknown'
                father_link = f'<a href="individual_{fam["father_id"]}.html" class="badge-link">{father_name}</a>' if fam['father_id'] else f'<span class="cell-text">{father_name}</span>'
                
                # Build mother name and link
                mother_parts = [fam['mother_given'] or '', fam['mother_surname'] or '']
                if fam.get('mother_suffix'):
                    mother_parts.append(fam['mother_suffix'])
                mother_name = ' '.join(filter(None, mother_parts)) or 'Unknown'
                mother_link = f'<a href="individual_{fam["mother_id"]}.html" class="badge-link">{mother_name}</a>' if fam['mother_id'] else f'<span class="cell-text">{mother_name}</span>'
                
                html += f"""
            <p><strong>Parents:</strong> {father_link} and {mother_link}</p>
"""
        
        # Spouses and Children section (inside the same detail-box)
        if spouse_families:
            for idx, fam in enumerate(spouse_families):
                # Build spouse name and link
                spouse_parts = [fam['spouse_given'] or '', fam['spouse_surname'] or '']
                if fam.get('spouse_suffix'):
                    spouse_parts.append(fam['spouse_suffix'])
                spouse_name = ' '.join(filter(None, spouse_parts)) or 'Unknown'
                spouse_link = f'<a href="individual_{fam["spouse_id"]}.html" class="badge-link">{spouse_name}</a>' if fam['spouse_id'] else f'<span class="cell-text">{spouse_name}</span>'
                
                # Add spacing between multiple spouses
                if idx > 0:
                    html += "<br>"
                
                html += f"""
            <p><strong>Spouse:</strong> {spouse_link}"""
                
                if fam['marriage_date']:
                    html += f" (m. {fam['marriage_date']})"
                html += "</p>\n"
                
                # Get children for this family
                cursor.execute("""
                    SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.birth_date
                    FROM child_family_xref cf
                    JOIN individuals i ON cf.child_id = i.individual_id
                    WHERE cf.family_id = %s
                    ORDER BY i.birth_date
                """, (fam['family_id'],))
                children = cursor.fetchall()
                
                if children:
                    html += """
            <p><strong>Children:</strong></p>
            <ul style="margin-top: 5px;">
"""
                    for child in children:
                        # Build child name with suffix
                        child_parts = [child['given_name'] or '', child['surname'] or '']
                        if child.get('suffix'):
                            child_parts.append(child['suffix'])
                        child_name = ' '.join(filter(None, child_parts)) or 'Unknown'
                        
                        html += f"                <li><a href=\"individual_{child['individual_id']}.html\" class=\"badge-link\">{child_name}</a>"
                        if child['birth_date']:
                            html += f" (b. {child['birth_date']})"
                        html += "</li>\n"
                    html += """
            </ul>
"""
        
        html += """
        </div>
        <div style="clear: both;"></div>
"""
        
        # Events section
        if events:
            html += """
    <div class="section-heading">Life Events</div>
        <div class="detail-box">
        <table>
            <thead>
                <tr>
                    <th>Event</th>
                    <th>Date</th>
                    <th>Place</th>
                </tr>
            </thead>
            <tbody>
"""
            for evt in events:
                # Create base event type as link to event detail page
                event_type_label = f'<a href="../events/event_{evt["event_id"]}.html">{format_event_type(evt["event_type"])}</a>'
                event_date = evt['event_date'] or ''
                
                # For NAME events, display the full name
                if evt['event_type'] == 'NAME' and evt['full_name']:
                    event_type_label += f': <span class="cell-text">{evt["full_name"]}</span>'
                # For MARR, DIV, ENGA, ANUL events, add spouse name and family link
                elif evt['event_type'] in ('MARR', 'DIV', 'ENGA', 'ANUL') and evt.get('family_id'):
                    # Build spouse name if available
                    if evt.get('spouse_id'):
                        spouse_parts = [evt.get('spouse_given') or '', evt.get('spouse_surname') or '']
                        if evt.get('spouse_suffix'):
                            spouse_parts.append(evt['spouse_suffix'])
                        spouse_name = ' '.join(filter(None, spouse_parts)) or 'Unknown'
                        event_type_label += f' to <a href="individual_{evt["spouse_id"]}.html" class="cell-text">{spouse_name}</a>'
                    
                    event_type_label += f' <a href="../families/family_{evt["family_id"]}.html" class="cell-text">(Family)</a>'
                    if evt['event_value']:
                        event_type_label += f' - <span class="cell-text">{evt["event_value"]}</span>'
                # For other events, add event value if present
                elif evt['event_value']:
                    event_type_label += f': <span class="cell-text">{evt["event_value"]}</span>'
                
                # Build place cell with link - need to look up place_id from place_name
                if evt['event_place']:
                    # Look up the place_id for this place name
                    cursor.execute("""
                        SELECT place_id FROM places WHERE place_name = %s LIMIT 1
                    """, (evt['event_place'],))
                    place_result = cursor.fetchone()
                    
                    if place_result:
                        place_cell = f'<a href="../places/place_{place_result["place_id"]}.html">{evt["event_place"]}</a>'
                    else:
                        place_cell = evt['event_place']
                else:
                    place_cell = ''
                
                # Get sources for this event with their attached media
                cursor.execute("""
                    SELECT s.source_id, s.title, s.author, s.publication_info
                    FROM sources s
                    JOIN event_source_xref esx ON s.source_id = esx.source_id
                    WHERE esx.event_id = %s
                """, (evt['event_id'],))
                event_sources = cursor.fetchall()
                
                # Build sources content with media
                sources_html = ""
                if event_sources:
                    sources_html = '<div style="margin-top: 8px; font-size: 0.9em;"><span class="cell-text" style="font-weight:bold;">Sources</span><br>'
                    for source in event_sources:
                        source_text = source['title'] or 'Untitled Source'
                        if source['author']:
                            source_text += f" by {source['author']}"
                        
                        # Convert source_id to plain filename (always links to the base source page)
                        clean_id = get_source_link_filename(source['source_id'])
                        sources_html += f'• <a href="../sources/source_{clean_id}.html">{source_text}</a><br>'
                        
                        # Get media attached to this source for this event (citation-level media)
                        cursor.execute("""
                            SELECT m.media_id, m.file_path, m.title
                            FROM media m
                            JOIN citation_media_xref cmx ON m.media_id = cmx.media_id
                            WHERE cmx.event_id = %s AND cmx.source_id = %s
                        """, (evt['event_id'], source['source_id']))
                        source_media = cursor.fetchall()
                        
                        if source_media:
                            sources_html += '<div style="margin-left: 20px; margin-top: 4px;">'
                            for media_item in source_media:
                                if media_item['file_path']:
                                    # Track this media as displayed
                                    displayed_media_ids.add(media_item['media_id'])
                                    
                                    from pathlib import Path
                                    # Remove 'media/' prefix if present
                                    file_path = media_item['file_path']
                                    if file_path.startswith('media/'):
                                        file_path = file_path[6:]  # Remove 'media/' prefix
                                    
                                    filename = Path(file_path).name
                                    thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                                    image_path = f"../images/{filename}"
                                    title = media_item['title'] or filename
                                    doc_exts = {'.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'}
                                    is_doc = Path(filename).suffix.lower() in doc_exts
                                    media_link = f"../media/media_{media_item['media_id']}.html" if is_doc else image_path
                                    
                                    sources_html += f'''
                                        <a href="{media_link}" target="_blank" title="{title}">
                                            <img src="{thumb_path}" alt="{title}" class="thumbnail" 
                                                 style="max-width: 80px; max-height: 80px; margin-right: 8px; margin-bottom: 4px; vertical-align: middle; border: 1px solid #ccc;"
                                                 onerror="this.onerror=null; this.src='{image_path}'; this.style.maxWidth='80px'; this.style.maxHeight='80px';">
                                        </a>
                                    '''
                            sources_html += '</div>'
                    
                    sources_html += '</div>'
                
                # Also check for event-level media (not attached to a specific source)
                cursor.execute("""
                    SELECT m.media_id, m.file_path, m.title
                    FROM media m
                    JOIN event_media_xref emx ON m.media_id = emx.media_id
                    WHERE emx.event_id = %s
                """, (evt['event_id'],))
                event_media = cursor.fetchall()
                
                if event_media:
                    if not sources_html:
                        sources_html = '<div style="margin-top: 8px; font-size: 0.9em;">'
                    else:
                        sources_html = sources_html[:-6]  # Remove closing </div>
                    
                    sources_html += '<strong>Event Media:</strong><br><div style="margin-left: 20px; margin-top: 4px;">'
                    for media_item in event_media:
                        if media_item['file_path']:
                            # Track this media as displayed
                            displayed_media_ids.add(media_item['media_id'])
                            
                            from pathlib import Path
                            # Remove 'media/' prefix if present
                            file_path = media_item['file_path']
                            if file_path.startswith('media/'):
                                file_path = file_path[6:]
                            
                            filename = Path(file_path).name
                            thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                            image_path = f"../images/{filename}"
                            title = media_item['title'] or filename
                            doc_exts = {'.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'}
                            is_doc = Path(filename).suffix.lower() in doc_exts
                            media_link = f"../media/media_{media_item['media_id']}.html" if is_doc else image_path
                            
                            sources_html += f'''
                                <a href="{media_link}" target="_blank" title="{title}">
                                    <img src="{thumb_path}" alt="{title}" class="thumbnail" 
                                         style="max-width: 80px; max-height: 80px; margin-right: 8px; margin-bottom: 4px; vertical-align: middle; border: 1px solid #ccc;"
                                         onerror="this.onerror=null; this.src='{image_path}'; this.style.maxWidth='80px'; this.style.maxHeight='80px';">
                                </a>
                            '''
                    sources_html += '</div></div>'
                
                html += f"""
                <tr>
                    <td>{event_type_label}{sources_html}</td>
                    <td>{'<span class="cell-text">' + event_date + '</span>' if event_date else ''}</td>
                    <td>{place_cell}</td>
                </tr>
"""
            html += """
            </tbody>
        </table>
        </div>
"""
        
        # Other Media section (media not already displayed in events)
        if media:
            # Filter out media already displayed in events
            other_media = [m for m in media if m['media_id'] not in displayed_media_ids]
            
            if other_media:
                html += """
    <div class="section-heading">Other Media</div>
        <div>
"""
                for m in other_media:
                    if m['file_path']:
                        filename = Path(m['file_path']).name
                        thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                        image_path = f"../images/{filename}"
                        title = m['title'] or filename
                        doc_exts = {'.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'}
                        is_doc = Path(filename).suffix.lower() in doc_exts
                        media_link = f"../media/media_{m['media_id']}.html" if is_doc else image_path
                        
                        html += f"""
            <a href="{media_link}">
                <img src="{thumb_path}" alt="{title}" class="thumbnail" onerror="this.src='{image_path}'">
            </a>
"""
                html += """
        </div>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'individuals' / f'individual_{individual_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(individuals)} individual pages")

def generate_families_index():
    """Generate two families index pages: sorted by husband and sorted by wife"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT f.family_id, f.marriage_date,
               h.individual_id as husband_id, h.given_name as husband_given, h.surname as husband_surname, h.suffix as husband_suffix,
               w.individual_id as wife_id, w.given_name as wife_given, w.surname as wife_surname, w.suffix as wife_suffix
        FROM families f
        LEFT JOIN individuals h ON f.husband_id = h.individual_id
        LEFT JOIN individuals w ON f.wife_id = w.individual_id
    """)
    families = cursor.fetchall()
    
    # Helper function to build name with suffix
    def build_name(given, surname, suffix):
        parts = [given or '', surname or '']
        if suffix:
            parts.append(suffix)
        return ' '.join(filter(None, parts)) or 'Unknown'
    
    # Helper function to build name with link
    badge_a = 'class="badge-link"'
    badge_span = 'class="cell-text"'

    def build_name_link(individual_id, given, surname, suffix):
        name = build_name(given, surname, suffix)
        if individual_id:
            return f'<a href="../individuals/individual_{individual_id}.html" {badge_a}>{name}</a>'
        return f'<span {badge_span}>{name}</span>' if name else ''
    
    # Helper function to build sort key (surname, suffix, given name)
    def sort_key_husband(fam):
        surname = (fam['husband_surname'] or '').upper()
        suffix = (fam['husband_suffix'] or '').upper()
        given = (fam['husband_given'] or '').upper()
        return (surname, given, suffix)
    
    def sort_key_wife(fam):
        surname = (fam['wife_surname'] or '').upper()
        suffix = (fam['wife_suffix'] or '').upper()
        given = (fam['wife_given'] or '').upper()
        return (surname, given, suffix)
    
    # Generate index sorted by husband
    families_by_husband = sorted(families, key=sort_key_husband)
    
    # Get unique first letters of husband surnames for navigation
    husband_letters = sorted(set((fam['husband_surname'] or '')[0].upper() 
                                  for fam in families_by_husband 
                                  if fam['husband_surname'] and fam['husband_surname'][0].isalpha()))
    
    html = get_html_header('Families by Husband', 1)
    html += f"""
        <h2>Families ({len(families)}) - Sorted by Husband's Surname</h2>
        <p><a href="index_by_wife.html" class="sort-view-link">View sorted by wife's surname →</a></p>
"""
    
    # Add alphabetical navigation with better spacing
    if husband_letters:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to:</strong> 
"""
        for letter in husband_letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Family ID</th>
                    <th>Husband</th>
                    <th>Wife</th>
                    <th>Marriage Date</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_letter = None
    for fam in families_by_husband:
        # Check if we need a letter divider
        surname = fam['husband_surname'] or ''
        if surname and surname[0].isalpha():
            first_letter = surname[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td colspan="4">
                        <strong>{first_letter}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        husband = build_name_link(fam['husband_id'], fam['husband_given'], fam['husband_surname'], fam.get('husband_suffix'))
        wife = build_name_link(fam['wife_id'], fam['wife_given'], fam['wife_surname'], fam.get('wife_suffix'))
        marriage = fam['marriage_date'] or ''
        
        html += f"""
                <tr id="{fam['family_id']}">
                    <td><a href="family_{fam['family_id']}.html">{fam['family_id']}</a></td>
                    <td>{husband}</td>
                    <td>{wife}</td>
                    <td><span class="cell-text">{marriage}</span></td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'families' / 'index.html', 'w') as f:
        f.write(html)
    
    # Generate index sorted by wife
    families_by_wife = sorted(families, key=sort_key_wife)
    
    # Get unique first letters of wife surnames for navigation
    wife_letters = sorted(set((fam['wife_surname'] or '')[0].upper() 
                              for fam in families_by_wife 
                              if fam['wife_surname'] and fam['wife_surname'][0].isalpha()))
    
    html = get_html_header('Families by Wife', 1)
    html += f"""
        <h2>Families ({len(families)}) - Sorted by Wife's Surname</h2>
        <p><a href="index.html" class="sort-view-link">← View sorted by husband's surname</a></p>
"""
    
    # Add alphabetical navigation with better spacing
    if wife_letters:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to:</strong> 
"""
        for letter in wife_letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Family ID</th>
                    <th>Wife</th>
                    <th>Husband</th>
                    <th>Marriage Date</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_letter = None
    for fam in families_by_wife:
        # Check if we need a letter divider
        surname = fam['wife_surname'] or ''
        if surname and surname[0].isalpha():
            first_letter = surname[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td colspan="4">
                        <strong>{first_letter}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        husband = build_name_link(fam['husband_id'], fam['husband_given'], fam['husband_surname'], fam.get('husband_suffix'))
        wife = build_name_link(fam['wife_id'], fam['wife_given'], fam['wife_surname'], fam.get('wife_suffix'))
        marriage = fam['marriage_date'] or ''
        
        html += f"""
                <tr id="{fam['family_id']}">
                    <td><a href="family_{fam['family_id']}.html">{fam['family_id']}</a></td>
                    <td>{husband}</td>
                    <td>{wife}</td>
                    <td><span class="cell-text">{marriage}</span></td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'families' / 'index_by_wife.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated families/index.html (by husband) with {len(families)} families")
    print(f"Generated families/index_by_wife.html (by wife) with {len(families)} families")

def get_primary_family_id(cursor, individual_id):
    """Return the family_id where individual_id is husband or wife, or None."""
    if not individual_id:
        return None
    cursor.execute("""
        SELECT family_id FROM families
        WHERE husband_id = %s OR wife_id = %s
        ORDER BY family_id
        LIMIT 1
    """, (individual_id, individual_id))
    row = cursor.fetchone()
    return row['family_id'] if row else None


def primary_family_badge(family_id, depth=1, individual_id=None):
    """Return a line with individual ID and/or primary family ID badges.
    Both are shown on the same line; either may be absent."""
    prefix = '../' * depth
    parts = []
    if individual_id:
        parts.append(
            f'<a href="{prefix}individuals/individual_{individual_id}.html" '
            f'class="badge-link" style="padding:1px 7px;font-size:0.85em;">{individual_id}</a>'
        )
    if family_id:
        parts.append(
            f'<a href="{prefix}families/family_{family_id}.html" '
            f'class="badge-link" style="padding:1px 7px;font-size:0.85em;">{family_id}</a>'
        )
    if not parts:
        return ''
    return '<br><small>' + '&nbsp;' + '&nbsp;'.join(parts) + '</small>'


def generate_family_pages():
    """Generate family detail pages with visual family tree diagram"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Get all families
    cursor.execute("SELECT family_id, husband_id, wife_id, marriage_date FROM families")
    families = cursor.fetchall()
    
    for fam in families:
        family_id = fam['family_id']
        husband_id = fam['husband_id']
        wife_id = fam['wife_id']
        marriage_date = fam['marriage_date'] or ''
        
        # Get husband info and his parents
        husband_name = 'Unknown'
        husband_birth = None
        husband_death = None
        husband_father = None
        husband_mother = None
        husband_parents_marriage = None
        if husband_id:
            cursor.execute("""
                SELECT given_name, surname, suffix, birth_date, death_date FROM individuals WHERE individual_id = %s
            """, (husband_id,))
            h = cursor.fetchone()
            if h:
                parts = [h['given_name'] or '', h['surname'] or '']
                if h.get('suffix'):
                    parts.append(h['suffix'])
                husband_name = ' '.join(filter(None, parts))
                husband_birth = h['birth_date']
                husband_death = h['death_date']
                
                # Get husband's parents and their marriage date
                cursor.execute("""
                    SELECT f.husband_id as father_id, f.wife_id as mother_id, f.marriage_date,
                           h.given_name as father_given, h.surname as father_surname, h.suffix as father_suffix,
                           h.birth_date as father_birth, h.death_date as father_death,
                           w.given_name as mother_given, w.surname as mother_surname, w.suffix as mother_suffix,
                           w.birth_date as mother_birth, w.death_date as mother_death
                    FROM child_family_xref cf
                    JOIN families f ON cf.family_id = f.family_id
                    LEFT JOIN individuals h ON f.husband_id = h.individual_id
                    LEFT JOIN individuals w ON f.wife_id = w.individual_id
                    WHERE cf.child_id = %s
                    LIMIT 1
                """, (husband_id,))
                h_parents = cursor.fetchone()
                if h_parents:
                    husband_father = h_parents
                    husband_mother = h_parents
                    husband_parents_marriage = h_parents['marriage_date']
        
        # Get wife info and her parents
        wife_name = 'Unknown'
        wife_birth = None
        wife_death = None
        wife_father = None
        wife_mother = None
        wife_parents_marriage = None
        if wife_id:
            cursor.execute("""
                SELECT given_name, surname, suffix, birth_date, death_date FROM individuals WHERE individual_id = %s
            """, (wife_id,))
            w = cursor.fetchone()
            if w:
                parts = [w['given_name'] or '', w['surname'] or '']
                if w.get('suffix'):
                    parts.append(w['suffix'])
                wife_name = ' '.join(filter(None, parts))
                wife_birth = w['birth_date']
                wife_death = w['death_date']
                
                # Get wife's parents and their marriage date
                cursor.execute("""
                    SELECT f.husband_id as father_id, f.wife_id as mother_id, f.marriage_date,
                           h.given_name as father_given, h.surname as father_surname, h.suffix as father_suffix,
                           h.birth_date as father_birth, h.death_date as father_death,
                           w.given_name as mother_given, w.surname as mother_surname, w.suffix as mother_suffix,
                           w.birth_date as mother_birth, w.death_date as mother_death
                    FROM child_family_xref cf
                    JOIN families f ON cf.family_id = f.family_id
                    LEFT JOIN individuals h ON f.husband_id = h.individual_id
                    LEFT JOIN individuals w ON f.wife_id = w.individual_id
                    WHERE cf.child_id = %s
                    LIMIT 1
                """, (wife_id,))
                w_parents = cursor.fetchone()
                if w_parents:
                    wife_father = w_parents
                    wife_mother = w_parents
                    wife_parents_marriage = w_parents['marriage_date']
        
        # Get children
        cursor.execute("""
            SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.birth_date, i.sex
            FROM child_family_xref cf
            JOIN individuals i ON cf.child_id = i.individual_id
            WHERE cf.family_id = %s
            ORDER BY i.birth_date
        """, (family_id,))
        children = cursor.fetchall()
        
        # Build HTML with visual family tree
        html = get_html_header(f'Family {family_id}', 1)
        html += """
        <div>
            <a href="index.html" class="return-to-index">← RETURN TO FAMILIES INDEX</a>
        </div>
"""
        html += f"""
        <h2>Family {family_id}: {husband_name} &amp; {wife_name}</h2>
        <p style="margin-top:-8px; font-size:0.85em;"><a href="index.html#{family_id}" class="return-to-index">Index by husband</a> &nbsp;|&nbsp; <a href="index_by_wife.html#{family_id}" class="return-to-index">Index by wife</a></p>
        
        <div class="family-tree">
"""
        
        # Grandparents row (if any exist)
        has_grandparents = husband_father or husband_mother or wife_father or wife_mother
        if has_grandparents:
            html += '<div class="tree-row" style="justify-content: space-around; align-items: flex-start;">'

            # --- Husband's parents (left side) ---
            html += '<div style="display: flex; flex-direction: column; gap: 6px; align-items: center;">'
            html += '<div style="font-size:1.1em; font-weight:bold; color:#0F3460; margin-bottom:4px;">Husband\'s Parents</div>'
            if husband_father and husband_father['father_id']:
                parts = [husband_father['father_given'] or '', husband_father['father_surname'] or '']
                if husband_father.get('father_suffix'):
                    parts.append(husband_father['father_suffix'])
                name = ' '.join(filter(None, parts))
                dates = ''
                if husband_father.get('father_birth'):
                    dates += f"<br><small>b. {husband_father['father_birth']}</small>"
                if husband_father.get('father_death'):
                    dates += f"<br><small>d. {husband_father['father_death']}</small>"
                html += f'<div class="person-box male"><a href="../individuals/individual_{husband_father["father_id"]}.html">{name}</a>{dates}{primary_family_badge(get_primary_family_id(cursor, husband_father["father_id"]), individual_id=husband_father["father_id"])}</div>'
            else:
                html += '<div class="person-box male" style="min-width:160px; text-align:center;"><span style="font-style:italic;opacity:0.7;color:white;">Unknown</span></div>'
            if husband_parents_marriage:
                html += f'<div style="font-size:1.0em; font-weight:bold; color:#2E8B57;">m. {husband_parents_marriage}</div>'
            if husband_mother and husband_mother['mother_id']:
                parts = [husband_mother['mother_given'] or '', husband_mother['mother_surname'] or '']
                if husband_mother.get('mother_suffix'):
                    parts.append(husband_mother['mother_suffix'])
                name = ' '.join(filter(None, parts))
                dates = ''
                if husband_mother.get('mother_birth'):
                    dates += f"<br><small>b. {husband_mother['mother_birth']}</small>"
                if husband_mother.get('mother_death'):
                    dates += f"<br><small>d. {husband_mother['mother_death']}</small>"
                html += f'<div class="person-box female"><a href="../individuals/individual_{husband_mother["mother_id"]}.html">{name}</a>{dates}{primary_family_badge(get_primary_family_id(cursor, husband_mother["mother_id"]), individual_id=husband_mother["mother_id"])}</div>'
            else:
                html += '<div class="person-box female" style="min-width:160px; text-align:center;"><span style="font-style:italic;opacity:0.7;color:white;">Unknown</span></div>'
            html += '</div>'

            # --- Wife's parents (right side) ---
            html += '<div style="display: flex; flex-direction: column; gap: 6px; align-items: center;">'
            html += '<div style="font-size:1.1em; font-weight:bold; color:#0F3460; margin-bottom:4px;">Wife\'s Parents</div>'
            if wife_father and wife_father['father_id']:
                parts = [wife_father['father_given'] or '', wife_father['father_surname'] or '']
                if wife_father.get('father_suffix'):
                    parts.append(wife_father['father_suffix'])
                name = ' '.join(filter(None, parts))
                dates = ''
                if wife_father.get('father_birth'):
                    dates += f"<br><small>b. {wife_father['father_birth']}</small>"
                if wife_father.get('father_death'):
                    dates += f"<br><small>d. {wife_father['father_death']}</small>"
                html += f'<div class="person-box male"><a href="../individuals/individual_{wife_father["father_id"]}.html">{name}</a>{dates}{primary_family_badge(get_primary_family_id(cursor, wife_father["father_id"]), individual_id=wife_father["father_id"])}</div>'
            else:
                html += '<div class="person-box male" style="min-width:160px; text-align:center;"><span style="font-style:italic;opacity:0.7;color:white;">Unknown</span></div>'
            if wife_parents_marriage:
                html += f'<div style="font-size:1.0em; font-weight:bold; color:#2E8B57;">m. {wife_parents_marriage}</div>'
            if wife_mother and wife_mother['mother_id']:
                parts = [wife_mother['mother_given'] or '', wife_mother['mother_surname'] or '']
                if wife_mother.get('mother_suffix'):
                    parts.append(wife_mother['mother_suffix'])
                name = ' '.join(filter(None, parts))
                dates = ''
                if wife_mother.get('mother_birth'):
                    dates += f"<br><small>b. {wife_mother['mother_birth']}</small>"
                if wife_mother.get('mother_death'):
                    dates += f"<br><small>d. {wife_mother['mother_death']}</small>"
                html += f'<div class="person-box female"><a href="../individuals/individual_{wife_mother["mother_id"]}.html">{name}</a>{dates}{primary_family_badge(get_primary_family_id(cursor, wife_mother["mother_id"]), individual_id=wife_mother["mother_id"])}</div>'
            else:
                html += '<div class="person-box female" style="min-width:160px; text-align:center;"><span style="font-style:italic;opacity:0.7;color:white;">Unknown</span></div>'
            html += '</div>'

            html += '</div>'  # end tree-row

            # Downward arrows
            html += '<div style="display: flex; justify-content: space-around; margin: 10px 0;">'
            html += '<div style="color: #5C4033; font-size: 2.5em; width: 50%; text-align: center;">↓</div>'
            html += '<div style="color: #5C4033; font-size: 2.5em; width: 50%; text-align: center;">↓</div>'
            html += '</div>'

        
        # Central couple (husband and wife) - Highlighted
        html += """
            <div class="tree-row">
                <div style="display: flex; gap: 10px; align-items: center;">
"""
        if husband_id:
            dates = ''
            if husband_birth:
                dates += f"<br><small>b. {husband_birth}</small>"
            if husband_death:
                dates += f"<br><small>d. {husband_death}</small>"
            html += f'<div class="person-box central-couple male"><a href="../individuals/individual_{husband_id}.html">{husband_name}</a>{dates}</div>'
        else:
            html += f'<div class="person-box central-couple male"><span style="font-style:italic;opacity:0.7;color:white;">Unknown</span></div>'
        
        html += f'<div style="color: #2E8B57; font-weight: bold; font-size: 1.2em; padding: 0 10px;">═══</div>'
        
        if wife_id:
            dates = ''
            if wife_birth:
                dates += f"<br><small>b. {wife_birth}</small>"
            if wife_death:
                dates += f"<br><small>d. {wife_death}</small>"
            html += f'<div class="person-box central-couple female"><a href="../individuals/individual_{wife_id}.html">{wife_name}</a>{dates}</div>'
        else:
            html += f'<div class="person-box central-couple female"><span style="font-style:italic;opacity:0.7;color:white;">Unknown</span></div>'
        
        html += """
                </div>
            </div>
"""
        
        if marriage_date:
            html += f'<div style="text-align: center; color: #2E8B57; font-size:1.1em; font-weight:bold; margin: 4px 0;">Married: {marriage_date}</div>'
        
        # Children row
        if children:
            html += """
            <div style="text-align: center; margin: 10px 0; color: #5C4033; font-size: 2.5em;">↓</div>
            <div class="tree-row" style="flex-wrap: wrap;">
"""
            for child in children:
                parts = [child['given_name'] or '', child['surname'] or '']
                if child.get('suffix'):
                    parts.append(child['suffix'])
                child_name = ' '.join(filter(None, parts))
                birth_info = f"<br><small>b. {child['birth_date']}</small>" if child['birth_date'] else ''
                child_fam_badge = primary_family_badge(get_primary_family_id(cursor, child['individual_id']), individual_id=child['individual_id'])
                
                html += f"""
                <div class="person-box {'male' if child.get('sex','') == 'M' else 'female' if child.get('sex','') == 'F' else ''}">
                    <a href="../individuals/individual_{child['individual_id']}.html">{child_name}</a>
                    {birth_info}{child_fam_badge}
                </div>
"""
            html += """
            </div>
"""
        
        html += """
        </div>
"""
        
        # Get and display family events (marriages, divorces, etc.)
        cursor.execute("""
            SELECT e.event_id, e.event_type, e.event_date, e.event_place
            FROM events e
            JOIN fam_event_xref fex ON e.event_id = fex.event_id
            WHERE fex.family_id = %s
            ORDER BY e.event_date
        """, (family_id,))
        family_events = cursor.fetchall()
        
        if family_events:
            html += """
        <div class="detail-box" style="margin-top: 30px;">
            <h3>Family Events</h3>
            <table>
                <thead>
                    <tr>
                        <th>Event Type</th>
                        <th>Date</th>
                        <th>Place</th>
                    </tr>
                </thead>
                <tbody>
"""
            for evt in family_events:
                event_type_display = format_event_type(evt['event_type'])
                event_link = f'<a href="../events/event_{evt["event_id"]}.html" class="badge-link">{event_type_display}</a>'
                event_date = f'<span class="cell-text">{evt["event_date"]}</span>' if evt['event_date'] else ''
                event_place = f'<span class="cell-text">{evt["event_place"]}</span>' if evt['event_place'] else ''
                
                html += f"""
                    <tr>
                        <td>{event_link}</td>
                        <td>{event_date}</td>
                        <td>{event_place}</td>
                    </tr>
"""
            
            html += """
                </tbody>
            </table>
        </div>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'families' / f'family_{family_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(families)} family detail pages")

def generate_events_index():
    """Generate the events index page"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Get all events with associated individual and place information
    cursor.execute("""
        SELECT e.event_id, e.event_type, e.event_date, e.event_place, p.place_id,
               i.individual_id, i.given_name, i.surname, i.suffix,
               f.family_id,
               h.individual_id as husband_id, h.given_name as husband_given, h.surname as husband_surname, h.suffix as husband_suffix,
               w.individual_id as wife_id, w.given_name as wife_given, w.surname as wife_surname, w.suffix as wife_suffix
        FROM events e
        LEFT JOIN indi_event_xref ix ON e.event_id = ix.event_id
        LEFT JOIN individuals i ON ix.individual_id = i.individual_id
        LEFT JOIN places p ON e.event_place = p.place_name
        LEFT JOIN fam_event_xref fx ON e.event_id = fx.event_id
        LEFT JOIN families f ON fx.family_id = f.family_id
        LEFT JOIN individuals h ON f.husband_id = h.individual_id
        LEFT JOIN individuals w ON f.wife_id = w.individual_id
    """)
    events = cursor.fetchall()
    
    # Sort events by parsed date
    events.sort(key=lambda evt: parse_gedcom_date(evt['event_date']))

    # Inline badge styles
    badge_a = 'class="badge-link"'
    badge_span = 'class="cell-text"'

    html = get_html_header('Events', 1)
    html += f"""
        <h2>Events ({len(events)})</h2>
        <p>Chronological list of all events in the database.</p>
        
        <table>
            <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Person</th>
                <th>Place</th>
            </tr>
"""
    
    for evt in events:
        # Format event type as link to event detail page
        event_type_link = f'<a href="event_{evt["event_id"]}.html" {badge_a}>{format_event_type(evt["event_type"])}</a>'

        # For family events (MARR, DIV, ENGA, ANUL) show both husband and wife
        if evt['event_type'] in ('MARR', 'DIV', 'ENGA', 'ANUL') and evt.get('family_id'):
            # Build husband name/link
            h_parts = [evt['husband_given'] or '', evt['husband_surname'] or '']
            if evt.get('husband_suffix'):
                h_parts.append(evt['husband_suffix'])
            h_name = ' '.join(filter(None, h_parts)) or 'Unknown'
            if evt.get('husband_id'):
                h_link = f'<a href="../individuals/individual_{evt["husband_id"]}.html" {badge_a}>{h_name}</a>'
            else:
                h_link = f'<span {badge_span}>{h_name}</span>'

            # Build wife name/link
            w_parts = [evt['wife_given'] or '', evt['wife_surname'] or '']
            if evt.get('wife_suffix'):
                w_parts.append(evt['wife_suffix'])
            w_name = ' '.join(filter(None, w_parts)) or 'Unknown'
            if evt.get('wife_id'):
                w_link = f'<a href="../individuals/individual_{evt["wife_id"]}.html" {badge_a}>{w_name}</a>'
            else:
                w_link = f'<span {badge_span}>{w_name}</span>'

            person_link = f'{h_link} &amp; {w_link}'

        else:
            # Build person name with suffix
            person_parts = [evt['given_name'] or '', evt['surname'] or '']
            if evt.get('suffix'):
                person_parts.append(evt['suffix'])
            person_name = ' '.join(filter(None, person_parts)) or 'Unknown'

            if evt.get('individual_id'):
                person_link = f'<a href="../individuals/individual_{evt["individual_id"]}.html" {badge_a}>{person_name}</a>'
            else:
                person_link = f'<span {badge_span}>{person_name}</span>'
        
        # Format place as link to place detail page if place_id exists
        if evt.get('place_id') and evt['event_place']:
            place_display = f'<a href="../places/place_{evt["place_id"]}.html" {badge_a}>{evt["event_place"]}</a>'
        else:
            place_display = f'<span {badge_span}>{evt["event_place"]}</span>' if evt.get('event_place') else ''
        
        html += f"""
            <tr>
                <td><span {badge_span}>{evt['event_date'] or ''}</span></td>
                <td>{event_type_link}</td>
                <td>{person_link}</td>
                <td>{place_display}</td>
            </tr>
"""
    
    html += """
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'events' / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated events/index.html with {len(events)} events")

def generate_event_pages():
    """Generate individual event detail pages"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT event_id, event_type, event_date, event_place, event_value FROM events")
    events = cursor.fetchall()
    
    for evt in events:
        event_id = evt['event_id']
        event_type = evt['event_type']
        event_date = evt['event_date'] or 'Date unknown'
        event_place = evt['event_place'] or 'Place unknown'
        event_value = evt['event_value'] or ''
        
        # Get individuals associated with this event
        cursor.execute("""
            SELECT i.individual_id, i.given_name, i.surname, i.suffix
            FROM individuals i
            JOIN indi_event_xref ix ON i.individual_id = ix.individual_id
            WHERE ix.event_id = %s
        """, (event_id,))
        individuals = cursor.fetchall()
        
        # Get families associated with this event (for MARR, DIV, etc.)
        cursor.execute("""
            SELECT f.family_id,
                   h.individual_id as husband_id, h.given_name as husband_given, h.surname as husband_surname, h.suffix as husband_suffix,
                   w.individual_id as wife_id, w.given_name as wife_given, w.surname as wife_surname, w.suffix as wife_suffix
            FROM families f
            JOIN fam_event_xref fx ON f.family_id = fx.family_id
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE fx.event_id = %s
        """, (event_id,))
        families = cursor.fetchall()
        
        # Get sources for this event
        cursor.execute("""
            SELECT s.source_id, s.title, s.author, s.publication_info
            FROM sources s
            JOIN event_source_xref esx ON s.source_id = esx.source_id
            WHERE esx.event_id = %s
        """, (event_id,))
        sources = cursor.fetchall()
        
        # Get media for this event
        cursor.execute("""
            SELECT m.media_id, m.file_path, m.title
            FROM media m
            JOIN event_media_xref emx ON m.media_id = emx.media_id
            WHERE emx.event_id = %s
        """, (event_id,))
        media = cursor.fetchall()
        
        # Get notes for this event
        cursor.execute("""
            SELECT n.note_id, n.note_text
            FROM notes n
            JOIN event_note_xref enx ON n.note_id = enx.note_id
            WHERE enx.event_id = %s
        """, (event_id,))
        notes = cursor.fetchall()
        
        # Build page title
        page_title = f"{format_event_type(event_type)} - {event_date}"
        
        html = get_html_header(page_title, 1)
        html += f"""
        <h2>Event: {format_event_type(event_type)}</h2>
        
        <div class="detail-box">
            <p><strong>Event ID:</strong> {event_id}</p>
            <p><strong>Date:</strong> {event_date}</p>
"""
        
        # Link to place if available
        if evt['event_place']:
            cursor.execute("""
                SELECT place_id FROM places WHERE place_name = %s LIMIT 1
            """, (evt['event_place'],))
            place_result = cursor.fetchone()
            
            if place_result:
                html += f'            <p><strong>Place:</strong> <a href="../places/place_{place_result["place_id"]}.html">{event_place}</a></p>\n'
            else:
                html += f'            <p><strong>Place:</strong> {event_place}</p>\n'
        
        if event_value:
            html += f'            <p><strong>Value:</strong> {event_value}</p>\n'
        
        html += """
        </div>
"""
        
        # People involved
        if individuals:
            html += """
        <h3>People Involved</h3>
        <ul>
"""
            for ind in individuals:
                # Build person name with suffix
                person_parts = [ind['given_name'] or '', ind['surname'] or '']
                if ind.get('suffix'):
                    person_parts.append(ind['suffix'])
                person_name = ' '.join(filter(None, person_parts)) or 'Unknown'
                
                html += f'            <li><a href="../individuals/individual_{ind["individual_id"]}.html">{person_name}</a></li>\n'
            
            html += """
        </ul>
"""
        
        # Families involved (for MARR, DIV events)
        if families:
            html += """
        <h3>Family</h3>
"""
            for fam in families:
                # Build husband name
                husband_parts = [fam['husband_given'] or '', fam['husband_surname'] or '']
                if fam.get('husband_suffix'):
                    husband_parts.append(fam['husband_suffix'])
                husband_name = ' '.join(filter(None, husband_parts)) or 'Unknown'
                
                # Build wife name
                wife_parts = [fam['wife_given'] or '', fam['wife_surname'] or '']
                if fam.get('wife_suffix'):
                    wife_parts.append(fam['wife_suffix'])
                wife_name = ' '.join(filter(None, wife_parts)) or 'Unknown'
                
                html += f"""
        <p><a href="../families/family_{fam['family_id']}.html">Family {fam['family_id']}</a>: """
                
                if fam['husband_id']:
                    html += f'<a href="../individuals/individual_{fam["husband_id"]}.html">{husband_name}</a>'
                else:
                    html += husband_name
                
                html += ' and '
                
                if fam['wife_id']:
                    html += f'<a href="../individuals/individual_{fam["wife_id"]}.html">{wife_name}</a>'
                else:
                    html += wife_name
                
                html += '</p>\n'
        
        # Sources with citation-level media
        if sources:
            html += """
    <div class="section-heading">Sources</div>
"""
            for source in sources:
                source_text = source['title'] or 'Untitled Source'
                if source['author']:
                    source_text += f" by {source['author']}"
                
                # Convert source_id to filename format (checks for dual-page sources)
                clean_id = get_source_link_filename(source['source_id'])
                html += f'        <div style="margin-bottom: 15px;">• <a href="../sources/source_{clean_id}.html">{source_text}</a><br>\n'
                
                # Get media attached to this source for this event (citation-level media)
                cursor.execute("""
                    SELECT m.media_id, m.file_path, m.title
                    FROM media m
                    JOIN citation_media_xref cmx ON m.media_id = cmx.media_id
                    WHERE cmx.event_id = %s AND cmx.source_id = %s
                """, (event_id, source['source_id']))
                source_media = cursor.fetchall()
                
                if source_media:
                    html += '        <div style="margin-left: 20px; margin-top: 8px;">\n'
                    for media_item in source_media:
                        if media_item['file_path']:
                            from pathlib import Path
                            # Remove 'media/' prefix if present
                            file_path = media_item['file_path']
                            if file_path.startswith('media/'):
                                file_path = file_path[6:]
                            
                            filename = Path(file_path).name
                            thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                            image_path = f"../images/{filename}"
                            title = media_item['title'] or filename
                            doc_exts = {'.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'}
                            is_doc = Path(filename).suffix.lower() in doc_exts
                            media_link = f"../media/media_{media_item['media_id']}.html" if is_doc else image_path
                            
                            html += f'''
            <a href="{media_link}" target="_blank" title="{title}">
                <img src="{thumb_path}" alt="{title}" class="thumbnail" 
                     style="max-width: 80px; max-height: 80px; margin-right: 8px; margin-bottom: 4px; vertical-align: middle; border: 1px solid #ccc;"
                     onerror="this.onerror=null; this.src='{image_path}'; this.style.maxWidth='80px'; this.style.maxHeight='80px';">
            </a>
'''
                    html += '        </div>\n'
                
                html += '        </div>\n'
        
        # Event-level media (not attached to a specific source)
        if media:
            html += """
        <h3>Event Media</h3>
        <div>
"""
            for m in media:
                if m['file_path']:
                    from pathlib import Path
                    # Remove 'media/' prefix if present
                    file_path = m['file_path']
                    if file_path.startswith('media/'):
                        file_path = file_path[6:]
                    
                    filename = Path(file_path).name
                    thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                    image_path = f"../images/{filename}"
                    title = m['title'] or filename
                    doc_exts = {'.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'}
                    is_doc = Path(filename).suffix.lower() in doc_exts
                    media_link = f"../media/media_{m['media_id']}.html" if is_doc else image_path
                    
                    html += f'''
            <a href="{media_link}" target="_blank" title="{title}">
                <img src="{thumb_path}" alt="{title}" class="thumbnail" 
                     style="max-width: 150px; max-height: 150px; margin: 10px; border: 2px solid #D4AF37;"
                     onerror="this.onerror=null; this.src='{image_path}'; this.style.maxWidth='150px'; this.style.maxHeight='150px';">
            </a>
'''
            
            html += """
        </div>
"""
        
        # Notes
        if notes:
            html += """
    <div class="section-heading">Notes</div>
"""
            for note in notes:
                note_text = note['note_text'] or ''
                # Convert newlines to <br> for HTML display
                note_html = note_text.replace('\n', '<br>')
                
                html += f"""
        <div class="detail-box">
            <p>{note_html}</p>
            <p><a href="../notes/note_{note['note_id']}.html">View full note</a></p>
        </div>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'events' / f'event_{event_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(events)} event detail pages")


def generate_places_index():
    """Generate four places index pages with different sort orders.
    
    Places with status S are routed to indexes based on which db_* fields are
    non-blank. A place only appears on an index if its relevant db_* field is
    populated. Places with no db_* fields populated go to Miscellaneous.
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT place_id, place_name,
               db_city, db_county, db_state, db_country
        FROM places
    """)
    all_places = cursor.fetchall()
    
    # Route each place based on status and which db_* fields are populated.
    # standard_places = has all 4 db_* fields (appears on all 4 indexes)
    # partial_places  = status S with some db_* fields (routed per-index)
    # misc_places     = no usable db_* data
    standard_places = []
    partial_places  = []
    misc_places     = []

    for place in all_places:
        city    = (place['db_city']    or '').strip()
        county  = (place['db_county']  or '').strip()
        state   = (place['db_state']   or '').strip()
        country = (place['db_country'] or '').strip()

        place['city']    = city
        place['county']  = county
        place['state']   = state
        place['country'] = country

        if any([city, county, state, country]):
            standard_places.append(place)
        elif any([city, county, state, country]):
            partial_places.append(place)
        else:
            misc_places.append(place)

    # misc_places also includes any non-S places that lack db_* data
    misc_places.sort(key=lambda p: p['place_name'].upper())

    misc_count     = len(misc_places)
    std_count      = len(standard_places)
    partial_count  = len(partial_places)
    total_count    = len(all_places)
    print(f"  Places: {std_count} standard, {partial_count} partial, {misc_count} miscellaneous, {total_count} total")

    # Diagnostic: show a few samples from each bucket
    if standard_places:
        sample = standard_places[0]
        print(f"  Sample standard: '{sample['place_name']}' → city='{sample['city']}' county='{sample['county']}' state='{sample['state']}' country='{sample['country']}'")
    if partial_places:
        sample = partial_places[0]
        print(f"  Sample partial:  '{sample['place_name']}' → city='{sample['city']}' county='{sample['county']}' state='{sample['state']}' country='{sample['country']}'")
    if misc_places:
        sample = misc_places[0]
        print(f"  Sample misc:     '{sample['place_name']}' → city='{sample['city']}'")
    # Build per-index lists by merging standard + applicable partial places
    def places_for_country():
        extras = [p for p in partial_places if p['country']]
        return standard_places + extras

    def places_for_state():
        extras = [p for p in partial_places if p['state']]
        return standard_places + extras

    def places_for_county():
        extras = [p for p in partial_places if p['county']]
        return standard_places + extras

    def places_for_city():
        extras = [p for p in partial_places if p['city']]
        return standard_places + extras

    # Helper function to write the Miscellaneous section at the bottom of a table
    def write_misc_section(html, misc_places):
        if not misc_places:
            return html
        html += f"""
                <tr class="letter-divider" id="misc">
                    <td>
                        <strong>Miscellaneous ({len(misc_places)})</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        for place in misc_places:
            html += f"""
                <tr>
                    <td><a href="place_{place['place_id']}.html">{place['place_name']}</a></td>
                </tr>
"""
        return html
    
    # Helper to add Misc to the Jump To bar
    def jump_to_misc_link(misc_places):
        if misc_places:
            return ' <a href="#misc">Misc</a>'
        return ''
    
    # ========================================================================
    # 1. BY COUNTRY INDEX (default: index.html)
    # ========================================================================
    places_by_country = sorted(
        [p for p in all_places if p['country']],
        key=lambda p: (p['country'].upper(), p['state'].upper(), p['county'].upper(), p['city'].upper())
    )
    
    letters = sorted(set(p['country'][0].upper() for p in places_by_country 
                         if p['country'] and p['country'][0].isalpha()))
    
    html = get_html_header('Places by Country', 1)
    html += f"""
        <div id="top"></div>
        <h2>Places ({len(places_by_country)}) - Sorted by Country</h2>
        <p>View by: <a href="index_by_state.html">State</a> | <a href="index_by_county.html">County</a> | <a href="index_by_city.html">City</a></p>
"""
    
    if letters or misc_places:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to Country:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += jump_to_misc_link(misc_places)
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Place Name</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_country = None
    for place in places_by_country:
        if place['country'] != current_country:
            current_country = place['country']
            first_letter = place['country'][0].upper() if place['country'] and place['country'][0].isalpha() else ''
            html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td>
                        <strong>{place['country']}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        html += f"""
                <tr>
                    <td><a href="place_{place['place_id']}.html">{place['place_name']}</a></td>
                </tr>
"""
    
    html = write_misc_section(html, misc_places)
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'places' / 'index.html', 'w') as f:
        f.write(html)
    
    # ========================================================================
    # 2. BY STATE INDEX
    # ========================================================================
    places_by_state = sorted(
        [p for p in all_places if p['state']],
        key=lambda p: (p['state'].upper(), p['county'].upper(), p['city'].upper())
    )
    
    letters = sorted(set(p['state'][0].upper() for p in places_by_state 
                         if p['state'] and p['state'][0].isalpha()))
    
    html = get_html_header('Places by State', 1)
    html += f"""
        <div id="top"></div>
        <h2>Places ({len(places_by_state)}) - Sorted by State</h2>
        <p>View by: <a href="index.html">Country</a> | <a href="index_by_county.html">County</a> | <a href="index_by_city.html">City</a></p>
"""
    
    if letters or misc_places:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to State:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += jump_to_misc_link(misc_places)
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Place Name</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_state = None
    for place in places_by_state:
        if place['state'] and place['state'] != current_state:
            current_state = place['state']
            first_letter = place['state'][0].upper() if place['state'][0].isalpha() else ''
            html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td>
                        <strong>{place['state']}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        html += f"""
                <tr>
                    <td><a href="place_{place['place_id']}.html">{place['place_name']}</a></td>
                </tr>
"""
    
    html = write_misc_section(html, misc_places)
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'places' / 'index_by_state.html', 'w') as f:
        f.write(html)
    
    # ========================================================================
    # 3. BY COUNTY INDEX
    # ========================================================================
    places_by_county = sorted(
        [p for p in all_places if p['county']],
        key=lambda p: (p['county'].upper(), p['city'].upper())
    )
    
    letters = sorted(set(p['county'][0].upper() for p in places_by_county 
                         if p['county'] and p['county'][0].isalpha()))
    
    html = get_html_header('Places by County', 1)
    html += f"""
        <div id="top"></div>
        <h2>Places ({len(places_by_county)}) - Sorted by County</h2>
        <p>View by: <a href="index.html">Country</a> | <a href="index_by_state.html">State</a> | <a href="index_by_city.html">City</a></p>
"""
    
    if letters or misc_places:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to County:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += jump_to_misc_link(misc_places)
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Place Name</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_county = None
    for place in places_by_county:
        if place['county'] and place['county'] != current_county:
            current_county = place['county']
            first_letter = place['county'][0].upper() if place['county'][0].isalpha() else ''
            html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td>
                        <strong>{place['county']}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        html += f"""
                <tr>
                    <td><a href="place_{place['place_id']}.html">{place['place_name']}</a></td>
                </tr>
"""
    
    html = write_misc_section(html, misc_places)
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'places' / 'index_by_county.html', 'w') as f:
        f.write(html)
    
    # ========================================================================
    # 4. BY CITY INDEX
    # ========================================================================
    places_by_city = sorted(
        [p for p in all_places if p['city']],
        key=lambda p: (p['city'].upper(), p['state'].upper(), p['county'].upper())
    )
    
    letters = sorted(set(p['city'][0].upper() for p in places_by_city 
                         if p['city'] and p['city'][0].isalpha()))
    
    html = get_html_header('Places by City', 1)
    html += f"""
        <div id="top"></div>
        <h2>Places ({len(places_by_city)}) - Sorted by City</h2>
        <p>View by: <a href="index.html">Country</a> | <a href="index_by_state.html">State</a> | <a href="index_by_county.html">County</a></p>
"""
    
    if letters or misc_places:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to City:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += jump_to_misc_link(misc_places)
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Place Name</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_city = None
    for place in places_by_city:
        if place['city'] != current_city:
            current_city = place['city']
            first_letter = place['city'][0].upper() if place['city'] and place['city'][0].isalpha() else ''
            html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td>
                        <strong>{place['city']}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        html += f"""
                <tr>
                    <td><a href="place_{place['place_id']}.html">{place['place_name']}</a></td>
                </tr>
"""
    
    html = write_misc_section(html, misc_places)
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'places' / 'index_by_city.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated places/index.html (by country) with {len(all_places)} places")
    print(f"Generated places/index_by_state.html with {len(all_places)} places")
    print(f"Generated places/index_by_county.html with {len(all_places)} places")
    print(f"Generated places/index_by_city.html with {len(all_places)} places")

def generate_place_pages():
    """Generate individual place detail pages"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("SELECT place_id, place_name FROM places")
    places = cursor.fetchall()
    
    for place in places:
        place_id = place['place_id']
        place_name = place['place_name']
        
        # Get all events at this place
        cursor.execute("""
            SELECT e.event_id, e.event_type, e.event_date,
                   i.individual_id, i.given_name, i.surname, i.suffix,
                   NULL as husband_id, NULL as husband_given, NULL as husband_surname, NULL as husband_suffix,
                   NULL as wife_id, NULL as wife_given, NULL as wife_surname, NULL as wife_suffix
            FROM events e
            LEFT JOIN indi_event_xref ix ON e.event_id = ix.event_id
            LEFT JOIN individuals i ON ix.individual_id = i.individual_id
            WHERE e.event_place = %s AND ix.event_id IS NOT NULL

            UNION

            SELECT e.event_id, e.event_type, e.event_date,
                   NULL as individual_id, NULL as given_name, NULL as surname, NULL as suffix,
                   h.individual_id as husband_id, h.given_name as husband_given, h.surname as husband_surname, h.suffix as husband_suffix,
                   w.individual_id as wife_id, w.given_name as wife_given, w.surname as wife_surname, w.suffix as wife_suffix
            FROM events e
            JOIN fam_event_xref fx ON e.event_id = fx.event_id
            JOIN families f ON fx.family_id = f.family_id
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE e.event_place = %s
        """, (place_name, place_name))
        events = cursor.fetchall()
        
        # Sort events by parsed date, then by person name
        def event_sort_key(evt):
            date_key = parse_gedcom_date(evt['event_date'])
            surname = (evt['surname'] or evt.get('husband_surname') or '').upper()
            given = (evt['given_name'] or evt.get('husband_given') or '').upper()
            suffix = (evt['suffix'] or evt.get('husband_suffix') or '').upper()
            return (date_key, surname, given, suffix)
        
        events.sort(key=event_sort_key)
        
        html = get_html_header(place_name, 1)
        html += """
        <div>
            <a href="index.html" class="return-to-index">← RETURN TO PLACES INDEX</a>
        </div>
"""
        html += f"""
        <h2>{place_name}</h2>
"""
        
        # Events at this location
        if events:
            html += f"""
        <h3>Events at this Location ({len(events)})</h3>
"""
            # Group events by date
            from itertools import groupby
            
            for event_date, date_events in groupby(events, key=lambda evt: evt['event_date'] or 'Unknown Date'):
                date_events_list = list(date_events)
                
                html += f"""
        <div class="detail-box">
            <h4>{event_date}</h4>
            <table style="margin-top: 10px;">
                <thead>
                    <tr>
                        <th>Event</th>
                        <th>Person(s)</th>
                    </tr>
                </thead>
                <tbody>
"""
                for evt in date_events_list:
                    event_type_label = format_event_type(evt['event_type'])
                    event_link = f'<a href="../events/event_{evt["event_id"]}.html">{event_type_label}</a>'
                    
                    # Build person name with suffix
                    if evt['individual_id']:
                        person_parts = [evt['given_name'] or '', evt['surname'] or '']
                        if evt.get('suffix'):
                            person_parts.append(evt['suffix'])
                        person_name = ' '.join(filter(None, person_parts)) or 'Unknown'
                        person_link = f'<a href="../individuals/individual_{evt["individual_id"]}.html">{person_name}</a>'
                    elif evt.get('husband_id') or evt.get('wife_id'):
                        parts = []
                        if evt.get('husband_id'):
                            h_parts = [evt['husband_given'] or '', evt['husband_surname'] or '']
                            if evt.get('husband_suffix'):
                                h_parts.append(evt['husband_suffix'])
                            h_name = ' '.join(filter(None, h_parts)) or 'Unknown'
                            parts.append(f'<a href="../individuals/individual_{evt["husband_id"]}.html">{h_name}</a>')
                        else:
                            parts.append('<span class="cell-text">Unknown</span>')
                        if evt.get('wife_id'):
                            w_parts = [evt['wife_given'] or '', evt['wife_surname'] or '']
                            if evt.get('wife_suffix'):
                                w_parts.append(evt['wife_suffix'])
                            w_name = ' '.join(filter(None, w_parts)) or 'Unknown'
                            parts.append(f'<a href="../individuals/individual_{evt["wife_id"]}.html">{w_name}</a>')
                        else:
                            parts.append('<span class="cell-text">Unknown</span>')
                        person_link = ' &amp; '.join(parts)
                    else:
                        person_link = '<span class="cell-text" Unknown</span>'
                    
                    html += f"""
                    <tr>
                        <td>{event_link}</td>
                        <td>{person_link}</td>
                    </tr>
"""
                
                html += """
                </tbody>
            </table>
        </div>
"""
        else:
            html += """
        <p>No events recorded at this location.</p>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'places' / f'place_{place_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(places)} place pages")

def generate_sources_index():
    """Generate the sources index page with alphabetical navigation and individual lists"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT source_id, title, author, publication_info
        FROM sources
        ORDER BY title
    """)
    sources = cursor.fetchall()
    
    # Get unique first letters for navigation
    letters = sorted(set(s['title'][0].upper() for s in sources 
                         if s['title'] and s['title'][0].isalpha()))
    
    html = get_html_header('Sources', 1)
    html += f"""
        <h2>Sources ({len(sources)})</h2>
        <p>Documentation and references cited in genealogical records.</p>
"""
    
    # Add alphabetical navigation
    if letters:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += """
        </div>
"""
    
    current_letter = None
    for source in sources:
        # Check if we need a letter divider
        title = source['title'] or 'Untitled'
        if title and title[0].isalpha():
            first_letter = title[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                html += f"""
        <div class="letter-divider-box">
            <h3 id="letter-{first_letter}">{first_letter}</h3>
            <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
        </div>
"""
        
        author = source['author'] or ''
        pub_info = source['publication_info'] or ''
        
        # Get individuals associated with this source (through events)
        # Sort by: surname, given name, suffix (proper ordering), then dates
        cursor.execute("""
            SELECT DISTINCT i.individual_id, i.given_name, i.surname, i.suffix
            FROM event_source_xref esx
            JOIN indi_event_xref iex ON esx.event_id = iex.event_id
            JOIN individuals i ON iex.individual_id = i.individual_id
            WHERE esx.source_id = %s
            ORDER BY 
                COALESCE(i.surname, ''),
                COALESCE(i.given_name, ''),
                CASE 
                    WHEN i.suffix IS NULL OR i.suffix = '' THEN 0
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) IN ('JR', 'SR') THEN 1
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) IN ('I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X') THEN 2
                    ELSE 3
                END,
                CASE 
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'JR' THEN 1
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'SR' THEN 2
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'I' THEN 1
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'II' THEN 2
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'III' THEN 3
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'IV' THEN 4
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'V' THEN 5
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'VI' THEN 6
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'VII' THEN 7
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'VIII' THEN 8
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'IX' THEN 9
                    WHEN UPPER(TRIM(TRAILING '.' FROM i.suffix)) = 'X' THEN 10
                    ELSE 0
                END,
                COALESCE(i.suffix, '')
        """, (source['source_id'],))
        individuals = cursor.fetchall()
        
        # Check if this source has any family events (marriages, divorces)
        cursor.execute("""
            SELECT COUNT(*) as family_event_count
            FROM event_source_xref esx
            JOIN events e ON esx.event_id = e.event_id
            JOIN fam_event_xref fex ON e.event_id = fex.event_id
            WHERE esx.source_id = %s
        """, (source['source_id'],))
        has_family_events = cursor.fetchone()['family_event_count'] > 0
        
        # Get media directly attached to this source
        cursor.execute("""
            SELECT m.media_id, m.file_path, m.title
            FROM source_media_xref smx
            JOIN media m ON smx.media_id = m.media_id
            WHERE smx.source_id = %s
        """, (source['source_id'],))
        media_items = list(cursor.fetchall())

        # Also get citation-level media (attached to events via this source)
        cursor.execute("""
            SELECT DISTINCT m.media_id, m.file_path, m.title
            FROM citation_media_xref cmx
            JOIN media m ON cmx.media_id = m.media_id
            WHERE cmx.source_id = %s
        """, (source['source_id'],))
        citation_media_items = cursor.fetchall()

        # Merge, avoiding duplicates by media_id
        existing_ids = {m['media_id'] for m in media_items}
        for m in citation_media_items:
            if m['media_id'] not in existing_ids:
                media_items.append(m)
                existing_ids.add(m['media_id'])
        
        # Build source entry with appropriate links
        clean_id = source['source_id'].replace('@', '').replace('S', 's')
        
        html += f"""
        <div style="margin: 15px 0; border: 1px solid var(--border-color); border-radius: 5px; overflow: hidden;">
            <div style="background-color: var(--navy-dark); padding: 10px 15px;">
                <h4 style="margin: 0; color: var(--gold-bright); font-size: 1.1em;">
"""
        
        if has_family_events:
            html += f"""
                    {title} &nbsp;&mdash;&nbsp; <a href="source_{clean_id}_by_husband.html" style="color: var(--gold-bright);">Sort by husband</a> | <a href="source_{clean_id}_by_wife.html" style="color: var(--gold-bright);">Sort by wife</a>
"""
        else:
            html += f"""
                    <a href="source_{clean_id}.html" style="color: var(--gold-bright);">{title}</a>
"""
        
        html += """
                </h4>
            </div>
            <div style="padding: 12px 15px; background-color: #fefefe;">
"""
        
        if author:
            html += f"""
            <div style="margin: 6px 0;"><span style="background-color: var(--navy-dark); color: white; font-weight: bold; padding: 2px 10px; border-radius: 4px; font-size: 0.85em; margin-right: 8px;">Author</span>{author}</div>
"""
        
        if pub_info:
            pub_info_clean = clean_publication_info(pub_info)
            if pub_info_clean:
                html += f"""
            <div style="margin: 6px 0;"><span style="background-color: var(--navy-dark); color: white; font-weight: bold; padding: 2px 10px; border-radius: 4px; font-size: 0.85em; margin-right: 8px;">Publication</span>{pub_info_clean}</div>
"""
        
        # Add media thumbnails if any
        if media_items:
            html += f"""
            <div style="margin-top: 10px; background-color: var(--navy-dark); color: white; font-weight: bold; padding: 4px 12px; border-radius: 4px; font-size: 0.9em; display: inline-block;">Media ({len(media_items)})</div>
            <div style="margin: 8px 0;">
"""
            for m in media_items:
                if m['file_path']:
                    from pathlib import Path
                    file_path = m['file_path']
                    if file_path.startswith('media/'):
                        file_path = file_path[6:]
                    
                    filename = Path(file_path).name
                    thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                    image_path = f"../images/{filename}"
                    media_title = m['title'] or filename
                    
                    html += f"""
                <div style="display:inline-block; text-align:center; vertical-align:top; margin:4px 6px;">
                    <a href="../media/media_{m['media_id']}.html">
                        <img src="{thumb_path}" alt="{media_title}" class="thumbnail"
                             onerror="this.onerror=null; this.src='{image_path}';" title="{media_title}">
                    </a>
                    <div style="font-size:0.75em; color:#333; max-width:100px; word-wrap:break-word; margin-top:3px;">{media_title}</div>
                </div>
"""
            html += """
            </div>
"""
        
        # Add individuals list
        if individuals:
            html += f"""
            <div style="margin-top: 10px; background-color: var(--navy-dark); color: white; font-weight: bold; padding: 4px 12px; border-radius: 4px; font-size: 0.9em; display: inline-block;">Referenced Individuals ({len(individuals)})</div>
            <div style="column-count: 3; column-gap: 15px; margin: 8px 0 4px 0;">
"""
            for ind in individuals:
                parts = [ind['given_name'] or '', ind['surname'] or '']
                if ind.get('suffix'):
                    parts.append(ind['suffix'])
                person_name = ' '.join(filter(None, parts)) or 'Unknown'
                
                html += f"""
                <div style="break-inside: avoid; margin-bottom: 3px;">
                    <a href="../individuals/individual_{ind['individual_id']}.html">{person_name}</a>
                </div>
"""
            
            html += """
            </div>
"""
        else:
            html += """
            <p style="margin: 10px 0 5px 0; color: #888;"><em>No individuals directly referenced</em></p>
"""
        
        html += """
            </div>
        </div>
"""
    
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'sources' / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated sources/index.html with {len(sources)} sources")

def generate_source_pages():
    """Generate source detail pages with all individuals listed alphabetically"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT source_id, title, author, publication_info, note_inline
        FROM sources
    """)
    sources = cursor.fetchall()
    
    for source in sources:
        source_id = source['source_id']
        title = source['title'] or 'Untitled Source'
        
        # Get individual events (births, deaths, etc.)
        cursor.execute("""
            SELECT DISTINCT e.event_id, e.event_type, e.event_date, 
                   i.individual_id, i.given_name, i.surname, i.suffix
            FROM event_source_xref esx
            JOIN events e ON esx.event_id = e.event_id
            LEFT JOIN indi_event_xref iex ON e.event_id = iex.event_id
            LEFT JOIN individuals i ON iex.individual_id = i.individual_id
            WHERE esx.source_id = %s
        """, (source_id,))
        individual_events = cursor.fetchall()
        
        # Get family events (marriages, divorces, etc.)
        cursor.execute("""
            SELECT DISTINCT e.event_id, e.event_type, e.event_date,
                   f.family_id,
                   h.individual_id as husband_id, h.given_name as husband_given, 
                   h.surname as husband_surname, h.suffix as husband_suffix,
                   w.individual_id as wife_id, w.given_name as wife_given,
                   w.surname as wife_surname, w.suffix as wife_suffix
            FROM event_source_xref esx
            JOIN events e ON esx.event_id = e.event_id
            JOIN fam_event_xref fex ON e.event_id = fex.event_id
            JOIN families f ON fex.family_id = f.family_id
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE esx.source_id = %s
        """, (source_id,))
        family_events = cursor.fetchall()
        
        # Get citation-specific media
        all_event_ids = [e['event_id'] for e in individual_events] + [e['event_id'] for e in family_events]
        citation_media = {}
        for event_id in all_event_ids:
            cursor.execute("""
                SELECT m.media_id, m.file_path, m.title
                FROM citation_media_xref cmx
                JOIN media m ON cmx.media_id = m.media_id
                WHERE cmx.event_id = %s AND cmx.source_id = %s
            """, (event_id, source_id))
            media_list = cursor.fetchall()
            if media_list:
                citation_media[event_id] = media_list
        
        # Generate single page
        _generate_source_page(source, individual_events, family_events, citation_media, cursor)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(sources)} source detail pages")


def _generate_source_page(source, individual_events, family_events, citation_media, cursor):
    """
    Generate a source detail page with all individuals listed alphabetically
    
    Args:
        source: Source record dict
        individual_events: List of individual event dicts
        family_events: List of family event dicts  
        citation_media: Dict mapping event_id to media list
        cursor: Database cursor
    """
    source_id = source['source_id']
    title = source['title'] or 'Untitled Source'
    
    html = get_html_header(f"Source: {title}", 1)
    html += """
        <div>
            <a href="index.html" class="return-to-index">← RETURN TO SOURCES INDEX</a>
        </div>
"""
    html += f"""
        <h2>Source: {title}</h2>
        
        <div class="detail-box">
            <p><strong>Source ID:</strong> {source_id}</p>
"""
    
    if source['author']:
        html += f"            <p><strong>Author:</strong> {source['author']}</p>\n"
    
    if source['publication_info']:
        pub_info_clean = clean_publication_info(source['publication_info'])
        if pub_info_clean:
            html += f"            <p><strong>Publication:</strong> {pub_info_clean}</p>\n"
    
    if source['note_inline']:
        note_html = convert_urls_to_links(source['note_inline'])
        html += f"            <p><strong>Notes:</strong> {note_html}</p>\n"
    
    html += """
        </div>
"""
    
    # Get repository information
    cursor.execute("""
        SELECT r.repository_id, r.name as repository_name, srx.call_number
        FROM source_repo_xref srx
        JOIN repositories r ON srx.repository_id = r.repository_id
        WHERE srx.source_id = %s
    """, (source_id,))
    repos = cursor.fetchall()
    
    if repos:
        html += """
        <h3>Repository</h3>
"""
        for repo in repos:
            html += f"""
        <div class="detail-box">
            <p><strong>Repository:</strong> <a href="../repositories/repository_{repo['repository_id']}.html">{repo['repository_name']}</a></p>
"""
            if repo['call_number']:
                html += f"            <p><strong>Call Number:</strong> {repo['call_number']}</p>\n"
            html += """
        </div>
"""
    
    # Build a list of all people involved and their events
    people_events = {}  # Key: individual_id, Value: list of events for that person
    
    # Add individual events
    for evt in individual_events:
        individual_id = evt['individual_id']
        if individual_id:
            if individual_id not in people_events:
                people_events[individual_id] = {
                    'given_name': evt['given_name'],
                    'surname': evt['surname'],
                    'suffix': evt['suffix'],
                    'events': []
                }
            people_events[individual_id]['events'].append({
                'event_id': evt['event_id'],
                'event_type': evt['event_type'],
                'event_date': evt['event_date'],
                'is_family_event': False
            })
    
    # Add family events - create entries for both spouses
    for evt in family_events:
        # Add to husband's events
        if evt['husband_id']:
            if evt['husband_id'] not in people_events:
                people_events[evt['husband_id']] = {
                    'given_name': evt['husband_given'],
                    'surname': evt['husband_surname'],
                    'suffix': evt['husband_suffix'],
                    'events': []
                }
            people_events[evt['husband_id']]['events'].append({
                'event_id': evt['event_id'],
                'event_type': evt['event_type'],
                'event_date': evt['event_date'],
                'is_family_event': True,
                'spouse_id': evt['wife_id'],
                'spouse_given': evt['wife_given'],
                'spouse_surname': evt['wife_surname'],
                'spouse_suffix': evt['wife_suffix']
            })
        
        # Add to wife's events
        if evt['wife_id']:
            if evt['wife_id'] not in people_events:
                people_events[evt['wife_id']] = {
                    'given_name': evt['wife_given'],
                    'surname': evt['wife_surname'],
                    'suffix': evt['wife_suffix'],
                    'events': []
                }
            people_events[evt['wife_id']]['events'].append({
                'event_id': evt['event_id'],
                'event_type': evt['event_type'],
                'event_date': evt['event_date'],
                'is_family_event': True,
                'spouse_id': evt['husband_id'],
                'spouse_given': evt['husband_given'],
                'spouse_surname': evt['husband_surname'],
                'spouse_suffix': evt['husband_suffix']
            })
    
    if people_events:
        # Sort people alphabetically by surname, given name, suffix
        sorted_people = sorted(people_events.items(), 
                              key=lambda x: create_name_sort_key(
                                  x[1]['surname'], 
                                  x[1]['given_name'], 
                                  x[1]['suffix']))
        
        html += f"""
        <h3>Citations ({sum(len(p[1]['events']) for p in sorted_people)})</h3>
        <table>
            <thead>
                <tr>
                    <th>Individual</th>
                    <th>Events</th>
                </tr>
            </thead>
            <tbody>
"""
        
        for individual_id, person_data in sorted_people:
            # Build person name
            parts = [person_data['given_name'] or '', person_data['surname'] or '']
            if person_data.get('suffix'):
                parts.append(person_data['suffix'])
            person_name = ' '.join(filter(None, parts)) or 'Unknown'
            person_link = f'<a href="../individuals/individual_{individual_id}.html">{person_name}</a>'
            
            # Build events list with dates and inline media
            events_html = '<div style="padding: 8px;">'
            
            # Sort events by date
            sorted_events = sorted(person_data['events'], 
                                  key=lambda e: parse_gedcom_date(e['event_date']))
            
            for evt in sorted_events:
                event_type = format_event_type(evt['event_type'])
                event_link = f'<a href="../events/event_{evt["event_id"]}.html">{event_type}</a>'
                event_date = evt['event_date'] or ''
                
                events_html += f'<div style="margin-bottom: 6px;"><strong>{event_link}</strong>'
                if event_date:
                    events_html += f' <span class="cell-text" style="margin-left:4px;">{event_date}</span>'
                
                # For family events, show spouse
                if evt['is_family_event'] and evt.get('spouse_id'):
                    spouse_parts = [evt['spouse_given'] or '', evt['spouse_surname'] or '']
                    if evt.get('spouse_suffix'):
                        spouse_parts.append(evt['spouse_suffix'])
                    spouse_name = ' '.join(filter(None, spouse_parts)) or 'Unknown'
                    spouse_link = f'<a href="../individuals/individual_{evt["spouse_id"]}.html">{spouse_name}</a>'
                    events_html += f' (with {spouse_link})'
                
                # Add media thumbnails inline with this event
                if evt['event_id'] in citation_media:
                    event_media = citation_media[evt['event_id']]
                    for media_item in event_media:
                        if media_item['file_path']:
                            file_name = os.path.basename(media_item['file_path'])
                            file_stem = os.path.splitext(file_name)[0]
                            thumb_filename = f"{file_stem}_thumb.jpg"
                            thumb_path = f"../thumbnails/{thumb_filename}"
                            media_title = media_item['title'] or 'Media'
                            events_html += f' <a href="../media/media_{media_item["media_id"]}.html" title="{media_title}"><img src="{thumb_path}" alt="{media_title}" style="max-width: 60px; max-height: 60px; margin: 0 2px; vertical-align: middle;" /></a>'
                
                events_html += '</div>'
            
            events_html += '</div>'
            
            html += f"""
                <tr>
                    <td>{person_link}</td>
                    <td>{events_html}</td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
"""
    
    # Get media attached to this source
    cursor.execute("""
        SELECT m.media_id, m.file_path, m.title
        FROM source_media_xref smx
        JOIN media m ON smx.media_id = m.media_id
        WHERE smx.source_id = %s
    """, (source_id,))
    media = cursor.fetchall()
    
    if media:
        html += """
    <div class="section-heading">Media</div>
        <div>
"""
        for m in media:
            if m['file_path']:
                from pathlib import Path
                file_path = m['file_path']
                if file_path.startswith('media/'):
                    file_path = file_path[6:]
                
                filename = Path(file_path).name
                thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                image_path = f"../images/{filename}"
                media_title = m['title'] or filename
                
                html += f"""
            <a href="../media/media_{m['media_id']}.html">
                <img src="{thumb_path}" alt="{media_title}" class="thumbnail" 
                     onerror="this.onerror=null; this.src='{image_path}';" title="{media_title}">
            </a>
"""
        html += """
        </div>
"""
    
    html += get_html_footer()
    
    # Write file
    clean_id = source_id.replace('@', '').replace('S', 's')
    with open(OUTPUT_DIR / 'sources' / f'source_{clean_id}.html', 'w') as f:
        f.write(html)

def generate_media_index():
    """Generate the media index page with alphabetical navigation"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT media_id, file_path, title
        FROM media
        ORDER BY title, file_path
    """)
    media_items = cursor.fetchall()
    
    # Get individuals for each media item
    media_individuals = {}
    for m in media_items:
        cursor.execute("""
            SELECT i.individual_id, i.given_name, i.surname, i.suffix
            FROM indi_media_xref im
            JOIN individuals i ON im.individual_id = i.individual_id
            WHERE im.media_id = %s
        """, (m['media_id'],))
        individuals = cursor.fetchall()
        # Sort using standardized name sorting
        individuals = sort_individuals_by_name(individuals)
        media_individuals[m['media_id']] = individuals
    
    # Get unique first letters for navigation (use title if available, otherwise filename)
    letters = set()
    for m in media_items:
        display_name = m['title'] or (Path(m['file_path']).name if m['file_path'] else 'Untitled')
        if display_name and display_name[0].isalpha():
            letters.add(display_name[0].upper())
    letters = sorted(letters)
    
    html = get_html_header('Media', 1)
    html += """
"""
    html += f"""
        <h2>Media ({len(media_items)})</h2>
        <p>Images, documents, and other media files.</p>
"""
    
    # Add alphabetical navigation
    if letters:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += """
        </div>
"""
    
    html += """
        <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(200px, 1fr)); gap: 20px; margin-top: 20px;">
"""
    
    current_letter = None
    for m in media_items:
        display_name = m['title'] or (Path(m['file_path']).name if m['file_path'] else 'Untitled')
        
        # Check if we need a letter divider (but keep it within the same grid)
        if display_name and display_name[0].isalpha():
            first_letter = display_name[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                # Add letter divider as a full-width grid item
                html += f"""
            <div id="letter-{first_letter}" class="letter-divider-box" style="grid-column: 1/-1; margin-top: 20px;">
                <strong style="font-size: 1.5em;">{first_letter}</strong>
                <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
            </div>
"""
        
        # Display media item
        if m['file_path']:
            from pathlib import Path
            file_path = m['file_path']
            if file_path.startswith('media/'):
                file_path = file_path[6:]
            
            filename = Path(file_path).name
            thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
            image_path = f"../images/{filename}"
            
            # Build individual names list
            individuals_html = ''
            individuals = media_individuals.get(m['media_id'], [])
            if individuals:
                individual_names = []
                for ind in individuals:
                    parts = [ind['given_name'] or '', ind['surname'] or '']
                    if ind.get('suffix'):
                        parts.append(ind['suffix'])
                    person_name = ' '.join(filter(None, parts)) or 'Unknown'
                    individual_names.append(f'<a href="individual_{ind["individual_id"]}.html" class="badge-link">{person_name}</a>')
                individuals_html = '<div style="margin-top: 5px; line-height: 2;">' + ' '.join(individual_names) + '</div>'
            
            file_ext = Path(filename).suffix.lower()
            if file_ext in ('.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'):
                icon_map = {
                    '.pdf': '📄', '.doc': '📝', '.docx': '📝',
                    '.htm': '🌐', '.html': '🌐', '.txt': '📃', '.rtf': '📝'
                }
                icon = icon_map.get(file_ext, '📎')
                html += f"""
            <div class="media-grid-card" style="background-color: white;">
                <a href="media_{m['media_id']}.html">
                    <img src="{thumb_path}" alt="{display_name}" 
                         style="max-width: 100%; height: 150px; object-fit: cover; border: 3px solid #FFD700; border-radius: 3px;"
                         onerror="this.onerror=null; this.parentElement.innerHTML='<div style=font-size:4em;padding:20px>{icon}</div><p style=margin-top:10px;font-size:0.9em>{display_name}</p>';">
                    <p style="margin-top: 10px; font-size: 0.9em;"><span class="cell-text">{display_name}</span></p>
                </a>
                {individuals_html}
            </div>
"""
            else:
                html += f"""
            <div class="media-grid-card" style="background-color: white;">
                <a href="media_{m['media_id']}.html">
                    <img src="{thumb_path}" alt="{display_name}" 
                         style="max-width: 100%; height: 150px; object-fit: cover; border: 3px solid #FFD700; border-radius: 3px;"
                         onerror="this.onerror=null; this.src='{image_path}'; this.style.objectFit='contain';">
                    <p style="margin-top: 10px; font-size: 0.9em;"><span class="cell-text">{display_name}</span></p>
                </a>
                {individuals_html}
            </div>
"""
    
    html += """
        </div>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'media' / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated media/index.html with {len(media_items)} media items")

def generate_media_pages():
    """Generate media detail pages"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT media_id, file_path, title, note_inline
        FROM media
    """)
    media_items = cursor.fetchall()
    
    for m in media_items:
        media_id = m['media_id']
        title = m['title'] or (Path(m['file_path']).name if m['file_path'] else 'Untitled')
        
        html = get_html_header(title, 1)
        html += """
"""
        html += f"""
        <h2>{title}</h2>
"""
        
        # Display the media file
        if m['file_path']:
            from pathlib import Path
            file_path = m['file_path']
            if file_path.startswith('media/'):
                file_path = file_path[6:]
            
            filename = Path(file_path).name
            image_path = f"../images/{filename}"
            file_ext = Path(filename).suffix.lower()
            
            # Document types get a download/view link instead of img tag
            if file_ext in ('.pdf', '.doc', '.docx', '.htm', '.html', '.txt', '.rtf'):
                icon_map = {
                    '.pdf': '📄', '.doc': '📝', '.docx': '📝',
                    '.htm': '🌐', '.html': '🌐', '.txt': '📃', '.rtf': '📝'
                }
                icon = icon_map.get(file_ext, '📎')
                thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                # Check if the actual file exists
                actual_file = OUTPUT_DIR / 'images' / filename
                if actual_file.exists():
                    html += f"""
        <div style="text-align: center; margin: 20px 0;">
            <div style="display: inline-block; padding: 30px; border: 2px solid var(--navy-medium, #16213E); border-radius: 8px; background-color: #f8f9fa;">
                <img src="{thumb_path}" alt="{title}" style="max-width: 200px; max-height: 200px; border: 3px solid #FFD700; border-radius: 3px;"
                     onerror="this.style.display=&apos;none&apos;; this.nextElementSibling.style.display=&apos;block&apos;">
                <div style="display: none; font-size: 4em;">{icon}</div>
                <p style="margin-top: 15px;"><a href="{image_path}" target="_blank" style="font-size: 1.2em; font-weight: bold;">📥 View / Download {file_ext.upper().replace(".","")} File</a></p>
                <p style="font-size: 0.9em; color: #666;">{filename}</p>
            </div>
        </div>
"""
                else:
                    html += f"""
        <div style="text-align: center; margin: 20px 0;">
            <div style="display: inline-block; padding: 30px; border: 2px solid #ccc; border-radius: 8px; background-color: #f8f9fa;">
                <div style="font-size: 4em;">{icon}</div>
                <p style="margin-top: 15px; font-size: 1.1em; color: #999;">File not available</p>
                <p style="font-size: 0.9em; color: #666;">{filename}</p>
            </div>
        </div>
"""
            else:
                html += f"""
        <div style="text-align: center; margin: 20px 0;">
            <img src="{image_path}" alt="{title}" class="media-detail-img" style="border: 3px solid #FFD700; border-radius: 5px;">
        </div>
"""
        
        html += f"""
        <div class="detail-box">
            <p><strong>Media ID:</strong> {media_id}</p>
"""
        
        if m['file_path']:
            html += f"            <p><strong>File:</strong> {Path(m['file_path']).name}</p>\n"
        
        if m['note_inline']:
            note_html = convert_urls_to_links(m['note_inline'])
            html += f"            <p><strong>Notes:</strong> {note_html}</p>\n"
        
        html += """
        </div>
"""
        
        # Get individuals associated with this media
        cursor.execute("""
            SELECT i.individual_id, i.given_name, i.surname, i.suffix
            FROM indi_media_xref im
            JOIN individuals i ON im.individual_id = i.individual_id
            WHERE im.media_id = %s
        """, (media_id,))
        individuals = cursor.fetchall()
        
        # Sort by name
        individuals = sort_individuals_by_name(individuals)
        
        if individuals:
            html += f"""
        <h3>Individuals ({len(individuals)})</h3>
        <p>People associated with this media:</p>
        <ul>
"""
            for ind in individuals:
                parts = [ind['given_name'] or '', ind['surname'] or '']
                if ind.get('suffix'):
                    parts.append(ind['suffix'])
                person_name = ' '.join(filter(None, parts)) or 'Unknown'
                
                html += f"""
            <li><a href="../individuals/individual_{ind['individual_id']}.html">{person_name}</a></li>
"""
            
            html += """
        </ul>
"""
        
        # Get events associated with this media
        cursor.execute("""
            SELECT DISTINCT e.event_id, e.event_type, e.event_date,
                   i.individual_id, i.given_name, i.surname, i.suffix
            FROM event_media_xref emx
            JOIN events e ON emx.event_id = e.event_id
            LEFT JOIN indi_event_xref iex ON e.event_id = iex.event_id
            LEFT JOIN individuals i ON iex.individual_id = i.individual_id
            WHERE emx.media_id = %s
            ORDER BY e.event_date, e.event_type
        """, (media_id,))
        events = cursor.fetchall()
        
        if events:
            html += f"""
        <h3>Events ({len(events)})</h3>
        <p>Events that reference this media:</p>
        <table>
            <thead>
                <tr>
                    <th>Event</th>
                    <th>Date</th>
                    <th>Individual</th>
                </tr>
            </thead>
            <tbody>
"""
            for evt in events:
                event_type = format_event_type(evt['event_type'])
                event_date = evt['event_date'] or ''
                
                if evt['individual_id']:
                    parts = [evt['given_name'] or '', evt['surname'] or '']
                    if evt.get('suffix'):
                        parts.append(evt['suffix'])
                    person_name = ' '.join(filter(None, parts)) or 'Unknown'
                    person_link = f'<a href="../individuals/individual_{evt["individual_id"]}.html">{person_name}</a>'
                else:
                    person_link = ''
                
                html += f"""
                <tr>
                    <td><span class="cell-text">{event_type}</span></td>
                    <td><span class="cell-text">{event_date}</span></td>
                    <td>{person_link}</td>
                </tr>
"""
            
            html += """
            </tbody>
        </table>
"""
        
        # Get sources that cite this media
        cursor.execute("""
            SELECT DISTINCT s.source_id, s.title
            FROM citation_media_xref cmx
            JOIN sources s ON cmx.source_id = s.source_id
            WHERE cmx.media_id = %s
        """, (media_id,))
        sources = cursor.fetchall()
        
        if sources:
            html += f"""
        <h3>Sources ({len(sources)})</h3>
        <p>Sources that reference this media:</p>
        <ul>
"""
            for src in sources:
                source_title = src['title'] or 'Untitled Source'
                # Convert source_id to filename format (checks for dual-page sources)
                clean_id = get_source_link_filename(src['source_id'])
                html += f"""
            <li><a href="../sources/source_{clean_id}.html">{source_title}</a></li>
"""
            html += """
        </ul>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'media' / f'media_{media_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(media_items)} media detail pages")

def generate_repositories_index():
    """Generate the repositories index page with alphabetical navigation"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT repository_id, name, address
        FROM repositories
        ORDER BY name
    """)
    repositories = cursor.fetchall()
    
    # Diagnostic: check for repositories referenced in source_repo_xref but not in repositories table
    cursor.execute("""
        SELECT DISTINCT srx.repository_id
        FROM source_repo_xref srx
        LEFT JOIN repositories r ON srx.repository_id = r.repository_id
        WHERE r.repository_id IS NULL
    """)
    orphan_repos = cursor.fetchall()
    if orphan_repos:
        print(f"  WARNING: {len(orphan_repos)} repository IDs referenced in source_repo_xref but missing from repositories table:")
        for orph in orphan_repos:
            print(f"    - {orph['repository_id']}")
    
    # Also check source_repo_xref count
    cursor.execute("SELECT COUNT(*) as cnt FROM source_repo_xref")
    srx_count = cursor.fetchone()['cnt']
    print(f"  Repositories table: {len(repositories)} entries, source_repo_xref: {srx_count} links")
    
    # Get unique first letters for navigation
    letters = sorted(set(r['name'][0].upper() for r in repositories 
                         if r['name'] and r['name'][0].isalpha()))
    
    html = get_html_header('Repositories', 1)
    html += f"""
        <h2>Repositories ({len(repositories)})</h2>
        <p>Archives, libraries, and institutions holding genealogical records.</p>
"""
    
    # Add alphabetical navigation
    if letters:
        html += """
        <div class="jump-to-bar">
            <strong>Jump to:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}">{letter}</a>'
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Repository Name</th>
                    <th>Address</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_letter = None
    for repo in repositories:
        # Check if we need a letter divider
        name = repo['name'] or 'Unnamed Repository'
        if name and name[0].isalpha():
            first_letter = name[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td colspan="2">
                        <strong>{first_letter}</strong>
                        <span style="float: right;"><a href="#">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        address = repo['address'] or ''
        
        html += f"""
                <tr>
                    <td><a href="repository_{repo['repository_id']}.html">{name}</a></td>
                    <td>{address}</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'repositories' / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated repositories/index.html with {len(repositories)} repositories")

def generate_repository_pages():
    """Generate repository detail pages"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT repository_id, name, address, note_inline
        FROM repositories
    """)
    repositories = cursor.fetchall()
    
    for repo in repositories:
        repository_id = repo['repository_id']
        name = repo['name'] or 'Unnamed Repository'
        
        html = get_html_header(name, 1)
        html += f"""
        <h2>{name}</h2>
        
        <div class="detail-box">
            <p><strong>Repository ID:</strong> {repository_id}</p>
"""
        
        if repo['address']:
            # Format address with line breaks
            address_lines = repo['address'].replace('\n', '<br>')
            html += f"            <p><strong>Address:</strong><br>{address_lines}</p>\n"
        
        if repo['note_inline']:
            note_html = convert_urls_to_links(repo['note_inline'])
            html += f"            <p><strong>Notes:</strong> {note_html}</p>\n"
        
        html += """
        </div>
"""
        
        # Get sources held at this repository
        cursor.execute("""
            SELECT s.source_id, s.title, s.author, srx.call_number
            FROM source_repo_xref srx
            JOIN sources s ON srx.source_id = s.source_id
            WHERE srx.repository_id = %s
            ORDER BY s.title
        """, (repository_id,))
        sources = cursor.fetchall()
        
        if sources:
            html += f"""
        <h3>Sources ({len(sources)})</h3>
        <p>Sources held at this repository:</p>
        <table>
            <thead>
                <tr>
                    <th>Title</th>
                    <th>Author</th>
                    <th>Call Number</th>
                </tr>
            </thead>
            <tbody>
"""
            for src in sources:
                source_title = src['title'] or 'Untitled Source'
                author = src['author'] or ''
                call_number = src['call_number'] or ''
                
                # Convert source_id to filename format (checks for dual-page sources)
                clean_id = get_source_link_filename(src['source_id'])
                html += f"""
                <tr>
                    <td><a href="../sources/source_{clean_id}.html">{source_title}</a></td>
                    <td style="color: var(--navy-dark); font-weight: bold;">{author}</td>
                    <td>{call_number}</td>
                </tr>
"""
            
            html += """
            </tbody>
        </table>
"""
        
        # Get all individuals referenced through sources at this repository
        cursor.execute("""
            SELECT DISTINCT i.individual_id, i.given_name, i.surname, i.suffix
            FROM source_repo_xref srx
            JOIN event_source_xref esx ON srx.source_id = esx.source_id
            JOIN indi_event_xref iex ON esx.event_id = iex.event_id
            JOIN individuals i ON iex.individual_id = i.individual_id
            WHERE srx.repository_id = %s
            LIMIT 50
        """, (repository_id,))
        individuals = cursor.fetchall()
        
        # Sort using standardized name sorting
        individuals = sort_individuals_by_name(individuals)
        
        if individuals:
            html += f"""
        <h3>Individuals Referenced</h3>
        <p>Individuals documented in sources at this repository (showing up to 50):</p>
        <div style="column-count: 3; column-gap: 20px;">
"""
            for ind in individuals:
                parts = [ind['given_name'] or '', ind['surname'] or '']
                if ind.get('suffix'):
                    parts.append(ind['suffix'])
                person_name = ' '.join(filter(None, parts)) or 'Unknown'
                
                html += f"""
            <div style="break-inside: avoid; margin-bottom: 5px;">
                <a href="../individuals/individual_{ind['individual_id']}.html">{person_name}</a>
            </div>
"""
            
            html += """
        </div>
"""
            if len(individuals) >= 50:
                html += """
        <p style="font-size: 0.9em; color: #666;"><em>Showing first 50 individuals. This repository may reference more.</em></p>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'repositories' / f'repository_{repository_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(repositories)} repository detail pages")

def generate_notes_index():
    """Generate the notes index page with alphabetical navigation"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT note_id, note_text
        FROM notes
        ORDER BY note_text
    """)
    notes = cursor.fetchall()
    
    # Get unique first letters for navigation (from first 50 chars of note text)
    letters = set()
    for n in notes:
        if n['note_text']:
            # Use first non-whitespace character
            text = n['note_text'].strip()
            if text and text[0].isalpha():
                letters.add(text[0].upper())
    letters = sorted(letters)
    
    html = get_html_header('Notes', 1)
    html += f"""
        <h2>Notes ({len(notes)})</h2>
        <p>Supplementary notes and research documentation.</p>
"""
    
    # Add alphabetical navigation
    if letters:
        html += """
        <div style="text-align: center; margin: 20px 0; padding: 15px; background-color: #f9f5f0; border-radius: 5px;">
            <strong>Jump to:</strong> 
"""
        for letter in letters:
            html += f' <a href="#letter-{letter}" style="margin: 0 8px; font-size: 1.2em; font-weight: bold;">{letter}</a>'
        html += """
        </div>
"""
    
    html += """
        <table>
            <thead>
                <tr>
                    <th>Note ID</th>
                    <th>Preview</th>
                </tr>
            </thead>
            <tbody>
"""
    
    current_letter = None
    for note in notes:
        # Check if we need a letter divider
        if note['note_text']:
            text = note['note_text'].strip()
            if text and text[0].isalpha():
                first_letter = text[0].upper()
                if first_letter != current_letter:
                    current_letter = first_letter
                    html += f"""
                <tr class="letter-divider" id="letter-{first_letter}">
                    <td colspan="2">
                        <strong>{first_letter}</strong>
                        <span style="float: right;"><a href="#" style="font-size: 0.9em;">RETURN TO TOP</a></span>
                    </td>
                </tr>
"""
        
        # Create preview (first 100 characters)
        preview = note['note_text'][:100] if note['note_text'] else ''
        if note['note_text'] and len(note['note_text']) > 100:
            preview += '...'
        
        # Convert URLs in preview to clickable links (showing the URL text)
        preview = convert_urls_to_links(preview, show_url=True)
        
        html += f"""
                <tr>
                    <td><a href="note_{note['note_id']}.html">{note['note_id']}</a></td>
                    <td>{preview}</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
"""
    html += get_html_footer()
    
    with open(OUTPUT_DIR / 'notes' / 'index.html', 'w') as f:
        f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated notes/index.html with {len(notes)} notes")

def generate_note_pages():
    """Generate note detail pages"""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT note_id, note_text
        FROM notes
    """)
    notes = cursor.fetchall()
    
    for note in notes:
        note_id = note['note_id']
        note_text = note['note_text'] or 'No text'
        
        # Convert URLs to clickable links
        note_text_html = convert_urls_to_links(note_text)
        
        # Use first line or first 50 chars as title
        title_text = note_text.split('\n')[0][:50]
        if len(title_text) < len(note_text.split('\n')[0]):
            title_text += '...'
        
        html = get_html_header(f'Note {note_id}', 1)
        html += f"""
        <h2>Note {note_id}</h2>
        
        <div class="detail-box">
            <p><strong>Note ID:</strong> {note_id}</p>
            <div style="white-space: pre-wrap; margin-top: 15px; padding: 15px; background-color: #fafafa; border-left: 3px solid #D4AF37; border-radius: 3px;">
{note_text_html}
            </div>
        </div>
"""
        
        # Get individuals associated with this note
        cursor.execute("""
            SELECT DISTINCT i.individual_id, i.given_name, i.surname, i.suffix
            FROM indi_citation_note_xref icnx
            JOIN individuals i ON icnx.individual_id = i.individual_id
            WHERE icnx.note_id = %s
        """, (note_id,))
        individuals = cursor.fetchall()
        
        # Sort using standardized name sorting
        individuals = sort_individuals_by_name(individuals)
        
        if individuals:
            html += f"""
        <h3>Individuals ({len(individuals)})</h3>
        <p>Individuals associated with this note:</p>
        <div style="column-count: 3; column-gap: 20px;">
"""
            for ind in individuals:
                parts = [ind['given_name'] or '', ind['surname'] or '']
                if ind.get('suffix'):
                    parts.append(ind['suffix'])
                person_name = ' '.join(filter(None, parts)) or 'Unknown'
                
                html += f"""
            <div style="break-inside: avoid; margin-bottom: 5px;">
                <a href="../individuals/individual_{ind['individual_id']}.html">{person_name}</a>
            </div>
"""
            
            html += """
        </div>
"""
        
        # Get families associated with this note
        cursor.execute("""
            SELECT DISTINCT f.family_id, 
                   h.given_name as husband_given, h.surname as husband_surname, h.suffix as husband_suffix,
                   w.given_name as wife_given, w.surname as wife_surname, w.suffix as wife_suffix
            FROM fam_note_xref fnx
            JOIN families f ON fnx.family_id = f.family_id
            LEFT JOIN individuals h ON f.husband_id = h.individual_id
            LEFT JOIN individuals w ON f.wife_id = w.individual_id
            WHERE fnx.note_id = %s
        """, (note_id,))
        families = cursor.fetchall()
        
        if families:
            html += f"""
        <h3>Families ({len(families)})</h3>
        <p>Families associated with this note:</p>
        <ul>
"""
            for fam in families:
                husband_parts = [fam['husband_given'] or '', fam['husband_surname'] or '']
                if fam.get('husband_suffix'):
                    husband_parts.append(fam['husband_suffix'])
                husband_name = ' '.join(filter(None, husband_parts)) or 'Unknown'
                
                wife_parts = [fam['wife_given'] or '', fam['wife_surname'] or '']
                if fam.get('wife_suffix'):
                    wife_parts.append(fam['wife_suffix'])
                wife_name = ' '.join(filter(None, wife_parts)) or 'Unknown'
                
                html += f"""
            <li><a href="../families/family_{fam['family_id']}.html">{husband_name} & {wife_name}</a></li>
"""
            
            html += """
        </ul>
"""
        
        # Get sources associated with this note
        cursor.execute("""
            SELECT DISTINCT s.source_id, s.title
            FROM source_note_xref snx
            JOIN sources s ON snx.source_id = s.source_id
            WHERE snx.note_id = %s
            ORDER BY s.title
        """, (note_id,))
        sources = cursor.fetchall()
        
        if sources:
            html += f"""
        <h3>Sources ({len(sources)})</h3>
        <p>Sources associated with this note:</p>
        <ul>
"""
            for src in sources:
                source_title = src['title'] or 'Untitled Source'
                # Convert source_id to filename format (checks for dual-page sources)
                clean_id = get_source_link_filename(src['source_id'])
                html += f"""
            <li><a href="../sources/source_{clean_id}.html">{source_title}</a></li>
"""
            
            html += """
        </ul>
"""
        
        # Get media associated with this note
        cursor.execute("""
            SELECT m.media_id, m.file_path, m.title
            FROM note_media_xref nmx
            JOIN media m ON nmx.media_id = m.media_id
            WHERE nmx.note_id = %s
        """, (note_id,))
        media = cursor.fetchall()
        
        if media:
            html += f"""
        <h3>Media ({len(media)})</h3>
        <div>
"""
            for m in media:
                if m['file_path']:
                    from pathlib import Path
                    file_path = m['file_path']
                    if file_path.startswith('media/'):
                        file_path = file_path[6:]
                    
                    filename = Path(file_path).name
                    thumb_path = f"../thumbnails/{Path(filename).stem}_thumb.jpg"
                    image_path = f"../images/{filename}"
                    media_title = m['title'] or filename
                    
                    html += f"""
            <a href="../media/media_{m['media_id']}.html">
                <img src="{thumb_path}" alt="{media_title}" class="thumbnail" 
                     onerror="this.onerror=null; this.src='{image_path}';" title="{media_title}">
            </a>
"""
            html += """
        </div>
"""
        
        html += get_html_footer()
        
        with open(OUTPUT_DIR / 'notes' / f'note_{note_id}.html', 'w') as f:
            f.write(html)
    
    cursor.close()
    conn.close()
    print(f"Generated {len(notes)} note detail pages")

def _query_person_name(row):
    """Build a display name from a DB row with given_name/surname/suffix."""
    parts = [row.get('given_name') or '', row.get('surname') or '']
    if row.get('suffix'):
        parts.append(row['suffix'])
    return ' '.join(filter(None, parts)) or 'Unknown'


# ---------------------------------------------------------------------------
# PRINT PAGE HELPER
# ---------------------------------------------------------------------------

def _print_page(title, description, columns, plain_rows, two_col=False):
    """
    Generate a self-contained print-optimized HTML page.
    plain_rows : list of lists of plain strings (no HTML links/badges)
    two_col    : if True, render as a two-column name list (first col only used)
    """
    ts = datetime.now().strftime('%Y-%m-%d %H:%M')
    col_css = 'column-count:2; column-gap:40px;' if two_col else ''

    if two_col:
        # Two-column: render as a simple list, one item per line
        items_html = ''.join(
            f'<div style="break-inside:avoid; padding:2px 0; border-bottom:1px solid #eee;">'
            f'{row[0]}'
            f'{"&nbsp;&nbsp;<span style=color:#666;font-size:0.9em>"+row[1]+"</span>" if len(row)>1 else ""}'
            f'</div>'
            for row in plain_rows
        )
        body_html = f'<div style="{col_css}">{items_html}</div>'
    else:
        th = ''.join(f'<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #333;">{c}</th>' for c in columns)
        rows_html = ''
        for i, row in enumerate(plain_rows):
            bg = 'background:#f9f9f9;' if i % 2 == 0 else ''
            td = ''.join(f'<td style="padding:5px 10px;border-bottom:1px solid #ddd;{bg}">{c}</td>' for c in row)
            rows_html += f'<tr>{td}</tr>'
        body_html = f'<table style="width:100%;border-collapse:collapse;"><thead><tr>{th}</tr></thead><tbody>{rows_html}</tbody></table>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>{title} - Print</title>
    <style>
        body {{ font-family: Georgia, serif; font-size: 11pt; color: #000;
                margin: 1cm 1.5cm; background: white; }}
        h1   {{ font-size: 16pt; border-bottom: 2px solid #000;
                padding-bottom: 6px; margin-bottom: 4px; }}
        .meta {{ font-size: 9pt; color: #555; margin-bottom: 16px; }}
        @media print {{ .no-print {{ display: none; }} }}
    </style>
</head>
<body onload="window.print()">
    <div class="no-print" style="background:#f0f0f0;padding:8px 16px;margin-bottom:16px;
         border-radius:4px;font-family:sans-serif;font-size:10pt;">
        <strong>Print preview</strong> — your browser's print dialog should open automatically.
        <button onclick="window.print()" style="margin-left:12px;padding:4px 10px;cursor:pointer;">Print</button>
        <button onclick="window.close()" style="margin-left:6px;padding:4px 10px;cursor:pointer;">Close</button>
    </div>
    <h1>{title}</h1>
    <div class="meta">Generated: {ts} &nbsp;&mdash;&nbsp; {len(plain_rows)} records &nbsp;&mdash;&nbsp; {description}</div>
    {body_html}
</body>
</html>"""


# ---------------------------------------------------------------------------
# SCREEN PAGE HELPERS
# ---------------------------------------------------------------------------

def _query_report_header(title, description, slug, depth=2):
    """Standard header for a query report page, with Screen/Print toolbar."""
    html  = get_html_header(title, depth)
    html += f"""
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:16px; flex-wrap:wrap;">
            <a href="../queries.html" class="return-to-index">← RETURN TO QUERIES</a>
            <a href="{slug}_print.html" target="_blank" class="return-to-index"
               style="background-color:var(--navy-medium);">🖨 Print Report</a>
        </div>
        <h2>{title}</h2>
        <p style="color: var(--navy-light); font-style: italic; margin-bottom: 20px;">
            Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} &nbsp;|&nbsp; {description}
        </p>
"""
    return html


def _query_table_with_filter(columns, rows_json, search_placeholder="Filter results..."):
    th_html = ''.join(f'<th>{c}</th>' for c in columns)
    return f"""
        <div style="margin-bottom: 12px;">
            <input type="text" id="qfilter" placeholder="{search_placeholder}"
                   style="padding: 8px 14px; width: 320px; border: 2px solid var(--navy-dark);
                          border-radius: 6px; font-size: 1em; outline: none;">
            <span id="qcount" style="margin-left: 12px; color: var(--navy-light); font-style: italic;"></span>
        </div>
        <table id="qtable">
            <thead><tr>{th_html}</tr></thead>
            <tbody id="qtbody"></tbody>
        </table>
        <script>
        (function() {{
            var data = {rows_json};
            var tbody = document.getElementById('qtbody');
            var counter = document.getElementById('qcount');
            function render(rows) {{
                tbody.innerHTML = '';
                rows.forEach(function(r) {{
                    var tr = document.createElement('tr');
                    tr.innerHTML = r.map(function(c) {{ return '<td>' + c + '</td>'; }}).join('');
                    tbody.appendChild(tr);
                }});
                counter.textContent = rows.length + ' of ' + data.length + ' rows';
            }}
            document.getElementById('qfilter').addEventListener('input', function() {{
                var q = this.value.toLowerCase();
                render(q ? data.filter(function(r) {{
                    return r.some(function(c) {{ return String(c).toLowerCase().indexOf(q) >= 0; }});
                }}) : data);
            }});
            render(data);
        }})();
        </script>
"""


# ============================================================================
# QUERY REPORT GENERATORS
# Each returns: (screen_html, print_html, count)
# ============================================================================

def _qr_missing_birth(cursor, prefix):
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.sex
        FROM individuals i
        WHERE NOT EXISTS (
            SELECT 1 FROM indi_event_xref ix
            JOIN events e ON ix.event_id = e.event_id
            WHERE ix.individual_id = i.individual_id
            AND e.event_type = 'BIRT'
            AND e.event_date IS NOT NULL AND e.event_date != ''
        )
        ORDER BY i.surname, i.given_name
    """)
    rows = cursor.fetchall()
    import json
    screen_rows = json.dumps([
        [f'<a href="{prefix}individuals/individual_{r["individual_id"]}.html">{_query_person_name(r)}</a>',
         r['individual_id'], r['sex'] or '']
        for r in rows
    ])
    plain_rows = [[_query_person_name(r), r['individual_id']] for r in rows]
    desc = f'{len(rows)} individuals have no birth date recorded'
    html  = _query_report_header('Missing Birth Date', desc, 'missing_birth')
    html += f'<p><strong>{len(rows)} individuals</strong> have no birth date recorded.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Sex'], screen_rows, 'Filter by name or ID...')
    html += get_html_footer()
    phtml = _print_page('Missing Birth Date', desc, ['Name', 'ID'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_missing_death(cursor, prefix):
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.sex
        FROM individuals i
        WHERE NOT EXISTS (
            SELECT 1 FROM indi_event_xref ix
            JOIN events e ON ix.event_id = e.event_id
            WHERE ix.individual_id = i.individual_id
            AND e.event_type = 'DEAT'
        )
        ORDER BY i.surname, i.given_name
    """)
    rows = cursor.fetchall()
    import json
    screen_rows = json.dumps([
        [f'<a href="{prefix}individuals/individual_{r["individual_id"]}.html">{_query_person_name(r)}</a>',
         r['individual_id'], r['sex'] or '']
        for r in rows
    ])
    plain_rows = [[_query_person_name(r), r['individual_id']] for r in rows]
    desc = f'{len(rows)} individuals have no death record'
    html  = _query_report_header('No Death Record', desc, 'missing_death')
    html += f'<p><strong>{len(rows)} individuals</strong> have no death record (may be living).</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Sex'], screen_rows, 'Filter by name or ID...')
    html += get_html_footer()
    phtml = _print_page('No Death Record', desc, ['Name', 'ID'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_missing_parents(cursor, prefix):
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.sex
        FROM individuals i
        WHERE NOT EXISTS (
            SELECT 1 FROM families f
            JOIN child_family_xref fc ON f.family_id = fc.family_id
            WHERE fc.child_id = i.individual_id
        )
        ORDER BY i.surname, i.given_name
    """)
    rows = cursor.fetchall()
    import json
    screen_rows = json.dumps([
        [f'<a href="{prefix}individuals/individual_{r["individual_id"]}.html">{_query_person_name(r)}</a>',
         r['individual_id'], r['sex'] or '']
        for r in rows
    ])
    plain_rows = [[_query_person_name(r), r['individual_id']] for r in rows]
    desc = f'{len(rows)} individuals have no parents in the database'
    html  = _query_report_header('No Parents Linked', desc, 'missing_parents')
    html += f'<p><strong>{len(rows)} individuals</strong> have no parents linked.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Sex'], screen_rows, 'Filter by name or ID...')
    html += get_html_footer()
    phtml = _print_page('No Parents Linked', desc, ['Name', 'ID'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_missing_sources(cursor, prefix):
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.sex
        FROM individuals i
        WHERE NOT EXISTS (
            SELECT 1 FROM indi_event_xref ix
            JOIN events e ON ix.event_id = e.event_id
            JOIN event_source_xref ecx ON e.event_id = ecx.event_id
            WHERE ix.individual_id = i.individual_id
        )
        ORDER BY i.surname, i.given_name
    """)
    rows = cursor.fetchall()
    import json
    screen_rows = json.dumps([
        [f'<a href="{prefix}individuals/individual_{r["individual_id"]}.html">{_query_person_name(r)}</a>',
         r['individual_id'], r['sex'] or '']
        for r in rows
    ])
    plain_rows = [[_query_person_name(r), r['individual_id']] for r in rows]
    desc = f'{len(rows)} individuals have no source citations'
    html  = _query_report_header('No Sources Cited', desc, 'missing_sources')
    html += f'<p><strong>{len(rows)} individuals</strong> have no source citations on any event.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Sex'], screen_rows, 'Filter by name or ID...')
    html += get_html_footer()
    phtml = _print_page('No Sources Cited', desc, ['Name', 'ID'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_duplicate_names(cursor, prefix):
    cursor.execute("""
        SELECT given_name, surname, COUNT(*) as cnt
        FROM individuals
        WHERE given_name IS NOT NULL AND surname IS NOT NULL
        GROUP BY given_name, surname
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC, surname, given_name
    """)
    groups = cursor.fetchall()
    import json
    screen_data = []
    plain_rows  = []
    for g in groups:
        cursor.execute("""
            SELECT individual_id, given_name, surname, suffix
            FROM individuals
            WHERE given_name = %s AND surname = %s
            ORDER BY suffix
        """, (g['given_name'], g['surname']))
        members = cursor.fetchall()
        ids_html = ' '.join(
            f'<a href="{prefix}individuals/individual_{m["individual_id"]}.html" class="badge-link">{m["individual_id"]}</a>'
            for m in members
        )
        ids_plain = ', '.join(m['individual_id'] for m in members)
        name = f"{g['given_name']} {g['surname']}"
        screen_data.append([name, str(g['cnt']), ids_html])
        plain_rows.append([name, str(g['cnt']), ids_plain])
    desc = f'{len(groups)} name combinations appear more than once'
    html  = _query_report_header('Duplicate Names', desc, 'duplicates')
    html += f'<p><strong>{len(groups)} name combinations</strong> appear more than once.</p>'
    html += _query_table_with_filter(['Name', 'Count', 'Individual IDs'],
                                     json.dumps(screen_data), 'Filter by name...')
    html += get_html_footer()
    phtml = _print_page('Duplicate Names', desc,
                        ['Name', 'Count', 'IDs'], plain_rows, two_col=False)
    return html, phtml, len(groups)


def _qr_date_problems(cursor, prefix):
    import json
    problems_screen = []
    problems_plain  = []

    def run_check(sql):
        cursor.execute(sql)
        return cursor.fetchall()

    shared_sql = """
        SELECT i.individual_id, i.given_name, i.surname, i.suffix,
               b.event_date as birth_date, d.event_date as death_date
        FROM individuals i
        JOIN indi_event_xref bx ON i.individual_id = bx.individual_id
        JOIN events b ON bx.event_id = b.event_id AND b.event_type = 'BIRT'
        JOIN indi_event_xref dx ON i.individual_id = dx.individual_id
        JOIN events d ON dx.event_id = d.event_id AND d.event_type = 'DEAT'
        WHERE b.event_date IS NOT NULL AND d.event_date IS NOT NULL
          AND b.event_date != '' AND d.event_date != ''
    """
    for r in run_check(shared_sql):
        birth = parse_gedcom_date(r['birth_date'])
        death = parse_gedcom_date(r['death_date'])
        name  = _query_person_name(r)
        link  = f'<a href="{prefix}individuals/individual_{r["individual_id"]}.html">{name}</a>'
        if death[0] < birth[0] and birth[0] < 9000:
            problems_screen.append([link, r['individual_id'], 'Death before birth',
                                    r['birth_date'], r['death_date']])
            problems_plain.append([name, r['individual_id'], 'Death before birth',
                                   r['birth_date'], r['death_date']])
        elif birth[0] < 9000 and death[0] < 9000 and death[0] - birth[0] > 110:
            age = death[0] - birth[0]
            problems_screen.append([link, r['individual_id'], f'Possible age {age} at death',
                                    r['birth_date'], r['death_date']])
            problems_plain.append([name, r['individual_id'], f'Possible age {age} at death',
                                   r['birth_date'], r['death_date']])

    desc = f'{len(problems_screen)} potential date inconsistencies found'
    html  = _query_report_header('Date Problems', desc, 'date_problems')
    html += f'<p><strong>{len(problems_screen)} potential date problems</strong> found.</p>'
    html += _query_table_with_filter(
        ['Name', 'ID', 'Problem', 'Birth Date', 'Death Date'],
        json.dumps(problems_screen), 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Date Problems', desc,
                        ['Name', 'ID', 'Problem', 'Birth', 'Death'],
                        problems_plain, two_col=False)
    return html, phtml, len(problems_screen)


def _qr_top_surnames(cursor, prefix):
    import json
    cursor.execute("""
        SELECT surname, COUNT(*) as cnt
        FROM individuals
        WHERE surname IS NOT NULL AND surname != ''
        GROUP BY surname
        ORDER BY cnt DESC
        LIMIT 50
    """)
    rows = cursor.fetchall()
    screen_rows = json.dumps([[r['surname'], str(r['cnt'])] for r in rows])
    plain_rows  = [[r['surname'], str(r['cnt'])] for r in rows]
    desc = 'Top 50 surnames by count'
    html  = _query_report_header('Top Surnames', desc, 'top_surnames')
    html += '<p>Top 50 surnames by number of individuals.</p>'
    html += _query_table_with_filter(['Surname', 'Count'], screen_rows, 'Filter by surname...')
    html += get_html_footer()
    phtml = _print_page('Top Surnames', desc, ['Surname', 'Count'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_births_by_decade(cursor, prefix):
    import json
    from collections import defaultdict
    cursor.execute("""
        SELECT e.event_date
        FROM indi_event_xref ix
        JOIN events e ON ix.event_id = e.event_id AND e.event_type = 'BIRT'
        WHERE e.event_date IS NOT NULL AND e.event_date != ''
    """)
    decades = defaultdict(int)
    for r in cursor.fetchall():
        year = parse_gedcom_date(r['event_date'])[0]
        if year < 9000:
            decades[(year // 10) * 10] += 1
    rows = [[str(d) + 's', str(decades[d])] for d in sorted(decades.keys())]
    screen_rows = json.dumps(rows)
    desc = 'Distribution of birth years by decade'
    html  = _query_report_header('Births by Decade', desc, 'births_by_decade')
    html += '<p>Number of individuals born in each decade.</p>'
    html += _query_table_with_filter(['Decade', 'Count'], screen_rows, 'Filter by decade...')
    html += get_html_footer()
    phtml = _print_page('Births by Decade', desc, ['Decade', 'Count'], rows, two_col=False)
    return html, phtml, len(decades)


def _qr_top_places(cursor, prefix):
    import json
    cursor.execute("""
        SELECT event_place, COUNT(*) as cnt
        FROM events
        WHERE event_place IS NOT NULL AND event_place != ''
        GROUP BY event_place
        ORDER BY cnt DESC
        LIMIT 50
    """)
    rows = cursor.fetchall()
    screen_rows = json.dumps([[r['event_place'], str(r['cnt'])] for r in rows])
    plain_rows  = [[r['event_place'], str(r['cnt'])] for r in rows]
    desc = 'Top 50 places by event count'
    html  = _query_report_header('Top Places', desc, 'top_places')
    html += '<p>Top 50 places by number of events.</p>'
    html += _query_table_with_filter(['Place', 'Event Count'], screen_rows, 'Filter by place...')
    html += get_html_footer()
    phtml = _print_page('Top Places', desc, ['Place', 'Count'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_relationship(cursor, prefix):
    """Interactive relationship explorer — no print version (interactive only)."""
    import json

    cursor.execute("SELECT individual_id, given_name, surname, suffix, sex FROM individuals")
    individuals = {r['individual_id']: r for r in cursor.fetchall()}

    cursor.execute("SELECT family_id, husband_id, wife_id FROM families")
    families = {r['family_id']: r for r in cursor.fetchall()}

    cursor.execute("SELECT family_id, child_id FROM child_family_xref")
    child_links = cursor.fetchall()

    child_to_families  = {}
    family_to_children = {}
    for cl in child_links:
        child_to_families.setdefault(cl['child_id'], []).append(cl['family_id'])
        family_to_children.setdefault(cl['family_id'], []).append(cl['child_id'])

    inds_js = {iid: {
        'name': _query_person_name(r),
        'sex':  r['sex'] or '',
        'url':  f'{prefix}individuals/individual_{iid}.html'
    } for iid, r in individuals.items()}

    fams_js = {fid: {
        'husband':  f['husband_id'] or '',
        'wife':     f['wife_id'] or '',
        'children': family_to_children.get(fid, [])
    } for fid, f in families.items()}

    ctf_js = child_to_families

    html  = _query_report_header('Relationship Explorer',
                                  'Find ancestors or descendants of any individual',
                                  'relationship')
    html += """
        <div style="background: var(--blue-very-light); border: 2px solid var(--navy-dark);
                    border-radius: 8px; padding: 20px; margin-bottom: 20px;">
            <label style="font-weight: bold; color: var(--navy-dark);">Individual ID:</label>
            <input type="text" id="rel-id" placeholder="e.g. @I36@"
                   style="margin: 0 10px; padding: 8px 14px; border: 2px solid var(--navy-dark);
                          border-radius: 6px; font-size: 1em; width: 160px;">
            <button onclick="runAncestors()" class="return-to-index"
                    style="margin-right: 8px; cursor: pointer;">Show Ancestors</button>
            <button onclick="runDescendants()" class="return-to-index"
                    style="cursor: pointer;">Show Descendants</button>
            <button onclick="printResult()" class="return-to-index"
                    style="margin-left: 16px; background-color:var(--navy-medium); cursor: pointer;">
                🖨 Print Result</button>
            <span id="rel-error" style="color: #c00; margin-left: 12px;"></span>
        </div>
        <div id="rel-result"></div>
"""

    html += f"""
        <script>
        var INDS = {json.dumps(inds_js)};
        var FAMS = {json.dumps(fams_js)};
        var CTF  = {json.dumps(ctf_js)};
        var lastTitle = '', lastRows = [];

        function getName(id) {{ return INDS[id] ? INDS[id].name : id; }}
        function getUrl(id)  {{ return INDS[id] ? INDS[id].url  : '#'; }}
        function link(id) {{
            return '<a href="' + getUrl(id) + '" class="badge-link">' + getName(id) + '</a>';
        }}

        function getInput() {{
            var raw = document.getElementById('rel-id').value.trim();
            document.getElementById('rel-error').textContent = '';
            if (!raw) {{ document.getElementById('rel-error').textContent = 'Please enter an ID.'; return null; }}
            var id = raw.startsWith('@') ? raw : '@' + raw + '@';
            if (!INDS[id]) {{ document.getElementById('rel-error').textContent = 'ID not found: ' + id; return null; }}
            return id;
        }}

        function runAncestors() {{
            var startId = getInput(); if (!startId) return;
            var result = [], visited = {{}};
            var queue = [[startId, 0]];
            while (queue.length) {{
                var item = queue.shift(), id = item[0], gen = item[1];
                if (visited[id]) continue;
                visited[id] = true;
                if (gen > 0) result.push([gen, id]);
                (CTF[id] || []).forEach(function(fid) {{
                    var fam = FAMS[fid];
                    if (!fam) return;
                    if (fam.husband) queue.push([fam.husband, gen + 1]);
                    if (fam.wife)    queue.push([fam.wife,    gen + 1]);
                }});
            }}
            result.sort(function(a,b){{ return a[0]-b[0] || getName(a[1]).localeCompare(getName(b[1])); }});
            lastTitle = 'Ancestors of ' + getName(startId);
            lastRows  = result.map(function(r){{ return [r[0], getName(r[1]), r[1]]; }});
            renderResult(lastTitle, ['Generation','Name','ID'],
                result.map(function(r){{ return [r[0], link(r[1]), r[1]]; }}));
        }}

        function runDescendants() {{
            var startId = getInput(); if (!startId) return;
            function getChildFams(id) {{
                return Object.keys(FAMS).filter(function(fid) {{
                    return FAMS[fid].husband === id || FAMS[fid].wife === id;
                }});
            }}
            var result = [], visited = {{}};
            var queue = [[startId, 0]];
            while (queue.length) {{
                var item = queue.shift(), id = item[0], gen = item[1];
                if (visited[id]) continue;
                visited[id] = true;
                if (gen > 0) result.push([gen, id]);
                getChildFams(id).forEach(function(fid) {{
                    (FAMS[fid].children || []).forEach(function(cid) {{
                        queue.push([cid, gen + 1]);
                    }});
                }});
            }}
            result.sort(function(a,b){{ return a[0]-b[0] || getName(a[1]).localeCompare(getName(b[1])); }});
            lastTitle = 'Descendants of ' + getName(startId);
            lastRows  = result.map(function(r){{ return [r[0], getName(r[1]), r[1]]; }});
            renderResult(lastTitle, ['Generation','Name','ID'],
                result.map(function(r){{ return [r[0], link(r[1]), r[1]]; }}));
        }}

        function renderResult(title, columns, rows) {{
            var div = document.getElementById('rel-result');
            if (!rows.length) {{
                div.innerHTML = '<p style="color:var(--navy-light);font-style:italic;">No results found.</p>';
                return;
            }}
            var thead = '<tr>' + columns.map(function(c){{ return '<th>' + c + '</th>'; }}).join('') + '</tr>';
            var tbody = rows.map(function(r) {{
                return '<tr>' + r.map(function(c){{ return '<td>' + c + '</td>'; }}).join('') + '</tr>';
            }}).join('');
            div.innerHTML = '<h3 style="color:var(--navy-dark);margin-bottom:10px;">' + title
                + ' <span style="font-size:0.8em;color:var(--navy-light);">(' + rows.length + ' individuals)</span></h3>'
                + '<table><thead>' + thead + '</thead><tbody>' + tbody + '</tbody></table>';
        }}

        function printResult() {{
            if (!lastRows.length) {{ alert('Run a report first.'); return; }}
            var ts = new Date().toLocaleString();
            var th = ['Generation','Name','ID'].map(function(c){{
                return '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid #333;">' + c + '</th>';
            }}).join('');
            var tbody = lastRows.map(function(r, i) {{
                var bg = i%2===0 ? 'background:#f9f9f9;' : '';
                var td = r.map(function(c){{
                    return '<td style="padding:5px 10px;border-bottom:1px solid #ddd;' + bg + '">' + c + '</td>';
                }}).join('');
                return '<tr>' + td + '</tr>';
            }}).join('');
            var html = '<!DOCTYPE html><html><head><meta charset="UTF-8"><title>' + lastTitle
                + '</title><style>body{{font-family:Georgia,serif;font-size:11pt;margin:1cm 1.5cm;}}'
                + 'h1{{font-size:16pt;border-bottom:2px solid #000;padding-bottom:6px;}}'
                + '.meta{{font-size:9pt;color:#555;margin-bottom:16px;}}'
                + 'table{{width:100%;border-collapse:collapse;}}'
                + '@media print{{.no-print{{display:none;}}}}'
                + '</style></head>'
                + '<body onload="window.print()">'
                + '<div class="no-print" style="background:#f0f0f0;padding:8px 16px;margin-bottom:16px;'
                + 'border-radius:4px;font-family:sans-serif;font-size:10pt;">'
                + '<strong>Print preview</strong> '
                + '<button onclick="window.print()" style="margin-left:12px;padding:4px 10px;">Print</button>'
                + '<button onclick="window.close()" style="margin-left:6px;padding:4px 10px;">Close</button></div>'
                + '<h1>' + lastTitle + '</h1>'
                + '<div class="meta">Generated: ' + ts + ' &mdash; ' + lastRows.length + ' individuals</div>'
                + '<table><thead><tr>' + th + '</tr></thead><tbody>' + tbody + '</tbody></table>'
                + '</body></html>';
            var w = window.open('', '_blank');
            w.document.write(html);
            w.document.close();
        }}
        </script>
"""
    html += get_html_footer()
    return html, None, 0   # No static print page for relationship explorer


# ============================================================================
# REPORT REGISTRY
# (slug, label, category, description, generator_fn)
# ============================================================================



# ============================================================================
# NEW QUERY REPORT GENERATORS — Data Quality & Date Checks
# ============================================================================

def _qr_invalid_name_chars(cursor, prefix):
    import json, re
    pattern = re.compile(r'[/\\|@#\[\]]')
    cursor.execute("""
        SELECT individual_id, given_name, surname, suffix, sex
        FROM individuals ORDER BY surname, given_name
    """)
    rows = cursor.fetchall()
    hits = []
    for r in rows:
        full = ' '.join(filter(None, [r['given_name'] or '', r['surname'] or '']))
        if pattern.search(full):
            hits.append((r, _query_person_name(r), full))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + r['individual_id'] + '.html">' + name + '</a>',
         r['individual_id'], full]
        for r, name, full in hits])
    plain_rows = [[name, r['individual_id'], full] for r, name, full in hits]
    desc = str(len(hits)) + ' individuals have invalid characters in their name'
    html  = _query_report_header('Invalid Name Characters', desc, 'invalid_name_chars')
    html += '<p>Flags names containing: <code>/ | @ # [ ]</code></p>'
    html += '<p><strong>' + str(len(hits)) + ' individuals</strong> found.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Full Name'], screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Invalid Name Characters', desc, ['Name', 'ID', 'Full Name'], plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_name_has_title(cursor, prefix):
    import json, re
    titles = ['Dr\\.', 'Rev\\.', 'Mr\\.', 'Mrs\\.', 'Ms\\.', 'Capt\\.', 'Gen\\.', 'Col\\.', 'Lt\\.',
              'Dr ', 'Rev ', 'Sir ', 'Lord ', 'Lady ']
    pattern = re.compile('(' + '|'.join(titles) + ')', re.IGNORECASE)
    cursor.execute("""
        SELECT individual_id, given_name, surname, suffix, sex
        FROM individuals ORDER BY surname, given_name
    """)
    rows = cursor.fetchall()
    hits = []
    for r in rows:
        given = r['given_name'] or ''
        m = pattern.search(given)
        if m:
            hits.append((r, _query_person_name(r), m.group(0).strip()))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + r['individual_id'] + '.html">' + name + '</a>',
         r['individual_id'], title]
        for r, name, title in hits])
    plain_rows = [[name, r['individual_id'], title] for r, name, title in hits]
    desc = str(len(hits)) + ' individuals may have a title in their given name'
    html  = _query_report_header('Name May Include Title', desc, 'name_has_title')
    html += '<p><strong>' + str(len(hits)) + ' individuals</strong> found.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Title Found'], screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Name May Include Title', desc, ['Name', 'ID', 'Title'], plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_same_surname_spouses(cursor, prefix):
    import json
    cursor.execute("""
        SELECT f.family_id,
               h.individual_id as hid, h.given_name as hgiven,
               h.surname as hsurname, h.suffix as hsuffix,
               w.individual_id as wid, w.given_name as wgiven,
               w.surname as wsurname, w.suffix as wsuffix
        FROM families f
        JOIN individuals h ON f.husband_id = h.individual_id
        JOIN individuals w ON f.wife_id    = w.individual_id
        WHERE h.surname IS NOT NULL AND h.surname != ''
          AND w.surname IS NOT NULL AND w.surname != ''
          AND h.surname = w.surname
        ORDER BY h.surname, h.given_name
    """)
    rows = cursor.fetchall()
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + r['hid'] + '.html">'
         + (r['hgiven'] or '') + ' ' + (r['hsurname'] or '') + '</a>',
         '<a href="' + prefix + 'individuals/individual_' + r['wid'] + '.html">'
         + (r['wgiven'] or '') + ' ' + (r['wsurname'] or '') + '</a>',
         r['hsurname'], r['family_id']]
        for r in rows])
    plain_rows = [[(r['hgiven'] or '') + ' ' + (r['hsurname'] or ''),
                   (r['wgiven'] or '') + ' ' + (r['wsurname'] or ''),
                   r['hsurname'], r['family_id']]
                  for r in rows]
    desc = str(len(rows)) + ' families where spouses share the same surname'
    html  = _query_report_header('Spouses Share Surname', desc, 'same_surname_spouses')
    html += '<p><strong>' + str(len(rows)) + ' families</strong> found.</p>'
    html += _query_table_with_filter(['Husband', 'Wife', 'Shared Surname', 'Family ID'],
                                     screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Spouses Share Surname', desc,
                        ['Husband', 'Wife', 'Surname', 'Family ID'], plain_rows, two_col=False)
    return html, phtml, len(rows)


def _qr_unknown_sex(cursor, prefix):
    import json
    cursor.execute("""
        SELECT individual_id, given_name, surname, suffix, sex
        FROM individuals
        WHERE sex IS NULL OR sex = '' OR (sex != 'M' AND sex != 'F')
        ORDER BY surname, given_name
    """)
    rows = cursor.fetchall()
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + r['individual_id'] + '.html">'
         + _query_person_name(r) + '</a>',
         r['individual_id'], r['sex'] or '(none)']
        for r in rows])
    plain_rows = [[_query_person_name(r), r['individual_id'], r['sex'] or '(none)'] for r in rows]
    desc = str(len(rows)) + ' individuals have unknown or missing sex'
    html  = _query_report_header('Unknown Sex', desc, 'unknown_sex')
    html += '<p><strong>' + str(len(rows)) + ' individuals</strong> found.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Sex Value'], screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Unknown Sex', desc, ['Name', 'ID', 'Sex'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _qr_isolated_individual(cursor, prefix):
    import json
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix, i.sex
        FROM individuals i
        WHERE NOT EXISTS (
            SELECT 1 FROM child_family_xref fc WHERE fc.child_id = i.individual_id
        )
        AND NOT EXISTS (
            SELECT 1 FROM families f
            WHERE f.husband_id = i.individual_id OR f.wife_id = i.individual_id
        )
        ORDER BY i.surname, i.given_name
    """)
    rows = cursor.fetchall()
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + r['individual_id'] + '.html">'
         + _query_person_name(r) + '</a>',
         r['individual_id'], r['sex'] or '']
        for r in rows])
    plain_rows = [[_query_person_name(r), r['individual_id'], r['sex'] or ''] for r in rows]
    desc = str(len(rows)) + ' individuals not connected to any other individual'
    html  = _query_report_header('Isolated Individuals', desc, 'isolated_individual')
    html += '<p><strong>' + str(len(rows)) + ' individuals</strong> have no parents, spouse, or children linked.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Sex'], screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Isolated Individuals', desc, ['Name', 'ID', 'Sex'], plain_rows, two_col=True)
    return html, phtml, len(rows)


def _parent_age_query(cursor, prefix, parent_col, age_check,
                      report_slug, report_title, desc_tmpl, col_label):
    import json
    sql = """
        SELECT c.individual_id as cid, c.given_name as cgiven,
               c.surname as csurname, c.suffix as csuffix,
               p.individual_id as pid, p.given_name as pgiven,
               p.surname as psurname, p.suffix as psuffix,
               cb.event_date as child_birth, pb.event_date as parent_birth
        FROM individuals c
        JOIN child_family_xref fc ON c.individual_id = fc.child_id
        JOIN families fam ON fc.family_id = fam.family_id
        JOIN individuals p ON fam.{pc} = p.individual_id
        JOIN indi_event_xref cbx ON c.individual_id = cbx.individual_id
        JOIN events cb ON cbx.event_id = cb.event_id AND cb.event_type = 'BIRT'
        JOIN indi_event_xref pbx ON p.individual_id = pbx.individual_id
        JOIN events pb ON pbx.event_id = pb.event_id AND pb.event_type = 'BIRT'
        WHERE cb.event_date IS NOT NULL AND cb.event_date != ''
          AND pb.event_date IS NOT NULL AND pb.event_date != ''
    """.format(pc=parent_col)
    cursor.execute(sql)
    hits = []
    for r in cursor.fetchall():
        cb = parse_gedcom_date(r['child_birth'])[0]
        pb = parse_gedcom_date(r['parent_birth'])[0]
        if cb < 9000 and pb < 9000 and age_check(cb - pb):
            cname = ' '.join(filter(None, [r['cgiven'] or '', r['csurname'] or '', r['csuffix'] or '']))
            pname = ' '.join(filter(None, [r['pgiven'] or '', r['psurname'] or '', r['psuffix'] or '']))
            hits.append((r['cid'], cname, r['pid'], pname,
                         r['child_birth'], r['parent_birth'], cb - pb))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + cid + '.html">' + cname + '</a>',
         '<a href="' + prefix + 'individuals/individual_' + pid + '.html">' + pname + '</a>',
         cbirth, pbirth, str(age)]
        for cid, cname, pid, pname, cbirth, pbirth, age in hits])
    plain_rows = [[cname, pname, cbirth, pbirth, str(age)]
                  for cid, cname, pid, pname, cbirth, pbirth, age in hits]
    desc = desc_tmpl.format(n=len(hits))
    html  = _query_report_header(report_title, desc, report_slug)
    html += '<p><strong>' + str(len(hits)) + ' individuals</strong> found.</p>'
    html += _query_table_with_filter(
        ['Child', col_label, 'Child Birth', col_label + ' Birth', col_label + ' Age'],
        screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page(report_title, desc,
                        ['Child', col_label, 'Child Birth', col_label + ' Birth', 'Age'],
                        plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_birth_after_father_death(cursor, prefix):
    import json
    cursor.execute("""
        SELECT c.individual_id as cid, c.given_name as cgiven,
               c.surname as csurname, c.suffix as csuffix,
               f.individual_id as fid, f.given_name as fgiven,
               f.surname as fsurname, f.suffix as fsuffix,
               cb.event_date as child_birth, fd.event_date as father_death
        FROM individuals c
        JOIN child_family_xref fc ON c.individual_id = fc.child_id
        JOIN families fam ON fc.family_id = fam.family_id
        JOIN individuals f ON fam.husband_id = f.individual_id
        JOIN indi_event_xref cbx ON c.individual_id = cbx.individual_id
        JOIN events cb ON cbx.event_id = cb.event_id AND cb.event_type = 'BIRT'
        JOIN indi_event_xref fdx ON f.individual_id = fdx.individual_id
        JOIN events fd ON fdx.event_id = fd.event_id AND fd.event_type = 'DEAT'
        WHERE cb.event_date IS NOT NULL AND cb.event_date != ''
          AND fd.event_date IS NOT NULL AND fd.event_date != ''
    """)
    hits = []
    for r in cursor.fetchall():
        cb = parse_gedcom_date(r['child_birth'])[0]
        fd = parse_gedcom_date(r['father_death'])[0]
        if cb < 9000 and fd < 9000 and cb > fd:
            cname = ' '.join(filter(None, [r['cgiven'] or '', r['csurname'] or '', r['csuffix'] or '']))
            fname = ' '.join(filter(None, [r['fgiven'] or '', r['fsurname'] or '', r['fsuffix'] or '']))
            hits.append((r['cid'], cname, r['fid'], fname, r['child_birth'], r['father_death']))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + cid + '.html">' + cname + '</a>',
         '<a href="' + prefix + 'individuals/individual_' + fid + '.html">' + fname + '</a>',
         cbirth, fdeath]
        for cid, cname, fid, fname, cbirth, fdeath in hits])
    plain_rows = [[cname, fname, cbirth, fdeath]
                  for cid, cname, fid, fname, cbirth, fdeath in hits]
    desc = str(len(hits)) + " individuals born after their father's death"
    html  = _query_report_header("Birth After Father's Death", desc, 'birth_after_father_death')
    html += '<p><strong>' + str(len(hits)) + ' individuals</strong> born after father died.</p>'
    html += _query_table_with_filter(['Child', 'Father', 'Child Birth', 'Father Death'],
                                     screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page("Birth After Father's Death", desc,
                        ['Child', 'Father', 'Child Birth', 'Father Death'], plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_birth_father_too_old(cursor, prefix):
    return _parent_age_query(cursor, prefix, 'husband_id', lambda a: a >= 80,
        'birth_father_too_old', 'Father Too Old at Birth',
        '{n} individuals born when father was 80 or older', 'Father')

def _qr_birth_father_too_young(cursor, prefix):
    return _parent_age_query(cursor, prefix, 'husband_id', lambda a: a < 13,
        'birth_father_too_young', 'Father Too Young at Birth',
        '{n} individuals born when father was under 13', 'Father')

def _qr_birth_mother_too_young(cursor, prefix):
    return _parent_age_query(cursor, prefix, 'wife_id', lambda a: a < 13,
        'birth_mother_too_young', 'Mother Too Young at Birth',
        '{n} individuals born when mother was under 13', 'Mother')

def _qr_birth_mother_too_old(cursor, prefix):
    return _parent_age_query(cursor, prefix, 'wife_id', lambda a: a >= 60,
        'birth_mother_too_old', 'Mother Too Old at Birth',
        '{n} individuals born when mother was 60 or older', 'Mother')


def _qr_burial_before_death(cursor, prefix):
    import json
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix,
               d.event_date as death_date, b.event_date as burial_date
        FROM individuals i
        JOIN indi_event_xref dx ON i.individual_id = dx.individual_id
        JOIN events d ON dx.event_id = d.event_id AND d.event_type = 'DEAT'
        JOIN indi_event_xref bx ON i.individual_id = bx.individual_id
        JOIN events b ON bx.event_id = b.event_id AND b.event_type = 'BURI'
        WHERE d.event_date IS NOT NULL AND d.event_date != ''
          AND b.event_date IS NOT NULL AND b.event_date != ''
    """)
    hits = []
    for r in cursor.fetchall():
        dd = parse_gedcom_date(r['death_date'])
        bd = parse_gedcom_date(r['burial_date'])
        if dd[0] < 9000 and bd[0] < 9000:
            if (bd[0], bd[1], bd[2]) < (dd[0], dd[1], dd[2]):
                hits.append((r['individual_id'], _query_person_name(r),
                             r['death_date'], r['burial_date']))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + iid + '.html">' + name + '</a>',
         iid, death, burial]
        for iid, name, death, burial in hits])
    plain_rows = [[name, iid, death, burial] for iid, name, death, burial in hits]
    desc = str(len(hits)) + ' individuals with burial date before death date'
    html  = _query_report_header('Burial Before Death', desc, 'burial_before_death')
    html += '<p><strong>' + str(len(hits)) + ' individuals</strong> found.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Death Date', 'Burial Date'],
                                     screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Burial Before Death', desc,
                        ['Name', 'ID', 'Death Date', 'Burial Date'], plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_age_at_death_over_120(cursor, prefix):
    import json
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix,
               b.event_date as birth_date, d.event_date as death_date
        FROM individuals i
        JOIN indi_event_xref bx ON i.individual_id = bx.individual_id
        JOIN events b ON bx.event_id = b.event_id AND b.event_type = 'BIRT'
        JOIN indi_event_xref dx ON i.individual_id = dx.individual_id
        JOIN events d ON dx.event_id = d.event_id AND d.event_type = 'DEAT'
        WHERE b.event_date IS NOT NULL AND b.event_date != ''
          AND d.event_date IS NOT NULL AND d.event_date != ''
    """)
    hits = []
    for r in cursor.fetchall():
        by = parse_gedcom_date(r['birth_date'])[0]
        dy = parse_gedcom_date(r['death_date'])[0]
        if by < 9000 and dy < 9000 and dy - by > 120:
            hits.append((r['individual_id'], _query_person_name(r),
                         r['birth_date'], r['death_date'], dy - by))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + iid + '.html">' + name + '</a>',
         iid, birth, death, str(age)]
        for iid, name, birth, death, age in hits])
    plain_rows = [[name, iid, birth, death, str(age)] for iid, name, birth, death, age in hits]
    desc = str(len(hits)) + ' individuals with age at death over 120'
    html  = _query_report_header('Age at Death Over 120', desc, 'age_at_death_over_120')
    html += '<p><strong>' + str(len(hits)) + ' individuals</strong> found.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Birth Date', 'Death Date', 'Age'],
                                     screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Age at Death Over 120', desc,
                        ['Name', 'ID', 'Birth', 'Death', 'Age'], plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_marriage_too_young(cursor, prefix):
    import json
    cursor.execute("""
        SELECT f.family_id,
               h.individual_id as hid, h.given_name as hgiven,
               h.surname as hsurname, h.suffix as hsuffix,
               w.individual_id as wid, w.given_name as wgiven,
               w.surname as wsurname, w.suffix as wsuffix,
               hb.event_date as husband_birth, wb.event_date as wife_birth,
               me.event_date as marriage_date
        FROM families f
        LEFT JOIN individuals h ON f.husband_id = h.individual_id
        LEFT JOIN individuals w ON f.wife_id    = w.individual_id
        JOIN fam_event_xref fex ON f.family_id = fex.family_id
        JOIN events me ON fex.event_id = me.event_id AND me.event_type = 'MARR'
        LEFT JOIN indi_event_xref hbx ON h.individual_id = hbx.individual_id
        LEFT JOIN events hb ON hbx.event_id = hb.event_id AND hb.event_type = 'BIRT'
        LEFT JOIN indi_event_xref wbx ON w.individual_id = wbx.individual_id
        LEFT JOIN events wb ON wbx.event_id = wb.event_id AND wb.event_type = 'BIRT'
        WHERE me.event_date IS NOT NULL AND me.event_date != ''
    """)
    hits = []
    seen = set()
    for r in cursor.fetchall():
        key = r['family_id']
        if key in seen:
            continue
        md = parse_gedcom_date(r['marriage_date'])[0]
        if md >= 9000:
            continue
        hname = ' '.join(filter(None, [r['hgiven'] or '', r['hsurname'] or '', r['hsuffix'] or ''])) or 'Unknown'
        wname = ' '.join(filter(None, [r['wgiven'] or '', r['wsurname'] or '', r['wsuffix'] or ''])) or 'Unknown'
        issues = []
        if r['husband_birth']:
            hb = parse_gedcom_date(r['husband_birth'])[0]
            if hb < 9000 and md - hb < 13:
                issues.append('Husband age ' + str(md - hb))
        if r['wife_birth']:
            wb = parse_gedcom_date(r['wife_birth'])[0]
            if wb < 9000 and md - wb < 13:
                issues.append('Wife age ' + str(md - wb))
        if issues:
            seen.add(key)
            hits.append((r['hid'], hname, r['wid'], wname,
                         r['marriage_date'], ', '.join(issues)))
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + (hid or '') + '.html">' + hname + '</a>' if hid else hname,
         '<a href="' + prefix + 'individuals/individual_' + (wid or '') + '.html">' + wname + '</a>' if wid else wname,
         mdate, issue]
        for hid, hname, wid, wname, mdate, issue in hits])
    plain_rows = [[hname, wname, mdate, issue]
                  for hid, hname, wid, wname, mdate, issue in hits]
    desc = str(len(hits)) + ' marriages where a spouse was under 13'
    html  = _query_report_header('Marriage Under Age 13', desc, 'marriage_too_young')
    html += '<p><strong>' + str(len(hits)) + ' marriages</strong> found.</p>'
    html += _query_table_with_filter(['Husband', 'Wife', 'Marriage Date', 'Issue'],
                                     screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Marriage Under Age 13', desc,
                        ['Husband', 'Wife', 'Marriage Date', 'Issue'], plain_rows, two_col=False)
    return html, phtml, len(hits)


def _qr_double_event(cursor, prefix):
    import json
    cursor.execute("""
        SELECT i.individual_id, i.given_name, i.surname, i.suffix,
               e.event_type, COUNT(*) as cnt,
               GROUP_CONCAT(COALESCE(e.event_date,'(no date)')
                            ORDER BY e.event_date SEPARATOR ' | ') as dates
        FROM individuals i
        JOIN indi_event_xref ix ON i.individual_id = ix.individual_id
        JOIN events e ON ix.event_id = e.event_id
        WHERE e.event_type IN ('BIRT','DEAT','BURI','BAPM','CHR')
        GROUP BY i.individual_id, i.given_name, i.surname, i.suffix, e.event_type
        HAVING COUNT(*) > 1
        ORDER BY i.surname, i.given_name, e.event_type
    """)
    rows = cursor.fetchall()
    screen_rows = json.dumps([
        ['<a href="' + prefix + 'individuals/individual_' + r['individual_id'] + '.html">'
         + _query_person_name(r) + '</a>',
         r['individual_id'], r['event_type'], str(r['cnt']), r['dates'] or '']
        for r in rows])
    plain_rows = [[_query_person_name(r), r['individual_id'],
                   r['event_type'], str(r['cnt']), r['dates'] or '']
                  for r in rows]
    desc = str(len(rows)) + ' possible duplicate events found'
    html  = _query_report_header('Possible Double Events', desc, 'double_event')
    html += ('<p>Flags individuals with more than one Birth, Death, Burial, '
             'Baptism, or Christening event.</p>')
    html += '<p><strong>' + str(len(rows)) + ' cases</strong> found.</p>'
    html += _query_table_with_filter(['Name', 'ID', 'Event Type', 'Count', 'Dates'],
                                     screen_rows, 'Filter...')
    html += get_html_footer()
    phtml = _print_page('Possible Double Events', desc,
                        ['Name', 'ID', 'Event Type', 'Count', 'Dates'],
                        plain_rows, two_col=False)
    return html, phtml, len(rows)


QUERY_REPORTS = [
    ('missing_birth',    'Missing Birth Date',    'Missing Data',
     'Individuals with no birth date recorded.',              _qr_missing_birth),
    ('missing_death',    'No Death Record',        'Missing Data',
     'Individuals with no death record (may be living).',     _qr_missing_death),
    ('missing_parents',  'No Parents Linked',      'Missing Data',
     'Individuals with no parents in the database.',          _qr_missing_parents),
    ('missing_sources',  'No Sources Cited',       'Missing Data',
     'Individuals with no source citations on any event.',    _qr_missing_sources),
    ('duplicates',       'Duplicate Names',         'Data Problems',
     'Name combinations that appear more than once.',         _qr_duplicate_names),
    ('date_problems',    'Date Problems',           'Data Problems',
     'Possible date errors: death before birth, extreme ages.', _qr_date_problems),
    ('top_surnames',     'Top Surnames',            'Statistics',
     'Top 50 surnames by number of individuals.',             _qr_top_surnames),
    ('births_by_decade', 'Births by Decade',        'Statistics',
     'Distribution of birth years by decade.',               _qr_births_by_decade),
    ('top_places',       'Top Places',              'Statistics',
     'Top 50 places by number of events.',                   _qr_top_places),
    ('relationship',     'Relationship Explorer',   'Relationships',
     'Find ancestors or descendants of any individual.',      _qr_relationship),
    ('invalid_name_chars',   'Invalid Name Characters',  'Data Quality',
     'Names containing /, |, @, #, [ or ].',                 _qr_invalid_name_chars),
    ('name_has_title',       'Name May Include Title',   'Data Quality',
     'Given names that appear to contain a title (Dr, Rev, Mr, etc.).',  _qr_name_has_title),
    ('same_surname_spouses', 'Spouses Share Surname',    'Data Quality',
     'Families where husband and wife have the same surname.',  _qr_same_surname_spouses),
    ('unknown_sex',          'Unknown Sex',              'Data Quality',
     "Individuals whose sex is blank or not M/F.",            _qr_unknown_sex),
    ('isolated_individual',  'Isolated Individuals',    'Data Quality',
     'Individuals with no parents, spouse, or children linked.', _qr_isolated_individual),
    ('birth_after_father_death', "Birth After Father's Death", 'Date Checks',
     "Individuals born after their father's recorded death date.", _qr_birth_after_father_death),
    ('birth_father_too_old',  'Father Too Old at Birth', 'Date Checks',
     'Father was 80 or older when child was born.',           _qr_birth_father_too_old),
    ('birth_father_too_young','Father Too Young at Birth','Date Checks',
     'Father was under 13 when child was born.',              _qr_birth_father_too_young),
    ('birth_mother_too_young','Mother Too Young at Birth','Date Checks',
     'Mother was under 13 when child was born.',              _qr_birth_mother_too_young),
    ('birth_mother_too_old',  'Mother Too Old at Birth', 'Date Checks',
     'Mother was 60 or older when child was born.',           _qr_birth_mother_too_old),
    ('burial_before_death',   'Burial Before Death',    'Date Checks',
     'Burial date is earlier than the death date.',           _qr_burial_before_death),
    ('age_at_death_over_120', 'Age at Death Over 120',  'Date Checks',
     'Individuals whose calculated age at death exceeds 120.', _qr_age_at_death_over_120),
    ('marriage_too_young',    'Marriage Under Age 13',  'Date Checks',
     'Marriages where either spouse was under 13.',           _qr_marriage_too_young),
    ('double_event',          'Possible Double Events', 'Date Checks',
     'Individuals with more than one Birth, Death, Burial, Baptism, or Christening.', _qr_double_event),
]


def generate_queries_page():
    """Generate queries index page and all individual report pages."""
    conn   = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    prefix = '../'

    report_counts = {}
    for slug, label, category, description, fn in QUERY_REPORTS:
        try:
            result = fn(cursor, prefix)
            screen_html, print_html, count = result

            # Write screen page
            with open(OUTPUT_DIR / 'queries' / f'{slug}.html', 'w') as f:
                f.write(screen_html)

            # Write print page (if provided)
            if print_html:
                with open(OUTPUT_DIR / 'queries' / f'{slug}_print.html', 'w') as f:
                    f.write(print_html)

            report_counts[slug] = count
            print(f"  Generated queries/{slug}.html  ({count} rows)")
        except Exception as e:
            import traceback
            print(f"  WARNING: Failed to generate queries/{slug}.html: {e}")
            traceback.print_exc()
            report_counts[slug] = '?'

    # ---- Queries index page ----
    html  = get_html_header('Queries', 0)
    html += """
        <h2>Queries &amp; Reports</h2>
        <p>All reports are generated fresh each time the website is built.
           Screen reports support live filtering; Print reports open in a
           new tab and trigger your browser's print dialog.</p>
"""
    categories = {}
    for slug, label, category, description, fn in QUERY_REPORTS:
        categories.setdefault(category, []).append((slug, label, description))

    for cat in ['Missing Data', 'Data Problems', 'Statistics', 'Relationships', 'Data Quality', 'Date Checks']:
        if cat not in categories:
            continue
        html += f"""
        <div class="detail-box">
        <div class="section-heading">{cat}</div>
        <table>
            <thead>
                <tr>
                    <th>Report</th>
                    <th>Description</th>
                    <th style="text-align:center;">Results</th>
                    <th style="text-align:center;">View</th>
                </tr>
            </thead>
            <tbody>
"""
        for slug, label, description in categories[cat]:
            count = report_counts.get(slug, '')
            count_disp = '' if (slug == 'relationship') else str(count)
            has_print  = slug != 'relationship'
            print_link = (f'&nbsp;<a href="queries/{slug}_print.html" target="_blank" '
                          f'class="badge-link" style="background:var(--navy-medium);">Print</a>')
            html += f"""
                <tr>
                    <td><a href="queries/{slug}.html">{label}</a></td>
                    <td><span class="cell-text">{description}</span></td>
                    <td style="text-align:center;"><span class="cell-text">{count_disp}</span></td>
                    <td style="text-align:center; white-space:nowrap;">
                        <a href="queries/{slug}.html" class="badge-link">Screen</a>
                        {''.join([print_link]) if has_print else ''}
                    </td>
                </tr>
"""
        html += """
            </tbody>
        </table>
        </div>
"""

    html += get_html_footer()
    with open(OUTPUT_DIR / 'queries.html', 'w') as f:
        f.write(html)

    cursor.close()
    conn.close()
    print(f"Generated queries.html ({len(QUERY_REPORTS)} reports)")


def main():
    """Main function to generate the entire website"""
    print("Starting genealogy website generation...")
    print(f"Output directory: {OUTPUT_DIR.absolute()}")
    
    # Load configuration first
    load_configuration()
    
    # Clean out old HTML files (preserves images)
    clean_old_website()
    
    # Create directory structure
    create_directories()
    
    # Copy assets (logo and CSS)
    copy_assets()
    
    # Process media files
    process_media_files()
    
    # Generate pages
    generate_index()
    generate_individuals_index()
    generate_individual_pages()
    generate_families_index()
    generate_family_pages()
    generate_events_index()
    generate_event_pages()
    generate_places_index()
    generate_place_pages()
    generate_sources_index()
    generate_source_pages()
    generate_media_index()
    generate_media_pages()
    generate_repositories_index()
    generate_repository_pages()
    generate_notes_index()
    generate_note_pages()
    generate_queries_page()
    
    print("\nWebsite generation complete!")
    print(f"Open {OUTPUT_DIR.absolute()}/index.html in your browser to view.")

if __name__ == '__main__':
    import sys
    parser = argparse.ArgumentParser(description='Generate genealogy website')
    parser.add_argument('gedcom', nargs='?', help='GEDCOM file (unused, for compatibility)')
    parser.add_argument('--config', help='Path to website_config.ini')
    args, _ = parser.parse_known_args()
    if args.config:
        CONFIG_FILE = Path(args.config)
    main()