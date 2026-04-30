#!/usr/bin/env python3
"""
FamilyGenealogy Setup Script
============================
Run this once to create your personal config.ini file.
Your config file can live anywhere on your computer — you just point
the scripts to it with --config when you run them.

Usage:
    python3 setup.py                       — run interactive setup
    python3 setup.py --print-form          — print a blank paper reference form
    python3 setup.py --print-form > my_answers.txt  — save form to file
    python3 setup.py --print-config        — write config_template.ini to edit directly
    python3 setup.py --print-config PATH   — write template to a specific path
"""

import os
import sys
import argparse
import configparser
from pathlib import Path


BANNER = """
╔══════════════════════════════════════════════════════════╗
║         FamilyGenealogy — Setup                          ║
╚══════════════════════════════════════════════════════════╝

This script helps you create your personal config.ini file.
It stores your folder paths, database credentials, and
website settings. You can re-run it any time to update.
"""

MENU = """
How would you like to proceed?

  1  Run the interactive setup wizard
  2  Print a blank paper form to fill out by hand
  3  Write a config_template.ini file to edit in a text editor
  4  Copy config_template.ini to config.ini

Enter 1, 2, 3, or 4 [default: 1]: """

DIVIDER = "\n" + "─" * 60 + "\n"


def ask(prompt, default=None, password=False):
    """Prompt the user for input, with optional default."""
    if default:
        full_prompt = f"  {prompt} [{default}]: "
    else:
        full_prompt = f"  {prompt}: "

    if password:
        import getpass
        val = getpass.getpass(full_prompt)
    else:
        val = input(full_prompt).strip()

    return val if val else (default or "")


def ask_path(prompt, must_exist=False, default=None):
    """Prompt for a folder path, expanding ~ and checking existence."""
    while True:
        raw = ask(prompt, default=default)
        if not raw:
            return ""
        path = os.path.expanduser(raw)
        if must_exist and not os.path.isdir(path):
            print(f"    ⚠  Folder not found: {path}")
            print(f"       Please create it first, or press Enter to skip.")
            retry = input("    Try again? [y/n]: ").strip().lower()
            if retry != "y":
                return path  # Return anyway, user acknowledged
        return path


def print_form():
    """Print a blank reference form showing all setup questions and defaults."""
    lines = [
        "",
        "╔══════════════════════════════════════════════════════════╗",
        "║       FamilyGenealogy — Setup Reference Form             ║",
        "╚══════════════════════════════════════════════════════════╝",
        "",
        "Fill in your answers before running setup.py interactively.",
        "Save this file, print it, or keep it open alongside setup.",
        "Press Enter during setup to accept any [default].",
        "",
        "─" * 60,
        "STEP 1 — Config File Location",
        "─" * 60,
        "  Config folder  [default: /Users/jamesrose/GRASP/GRASP_User]",
        "  Your answer: ________________________________________________",
        "",
        "─" * 60,
        "STEP 2 — MySQL Database Settings",
        "─" * 60,
        "  MySQL host      [default: localhost]    : ____________________",
        "  MySQL user      [default: genealogy]    : ____________________",
        "  MySQL password  [default: lizrrose]     : ____________________",
        "  MySQL database  [default: genealogy]    : ____________________",
        "",
        "─" * 60,
        "STEP 3 — Website Settings",
        "─" * 60,
        "  Site header name    [default: Liz's Genealogy]     : _________",
        "  Welcome family name [default: Liz's Genealogy]     : _________",
        "  Copyright line      [default: All rights reserved.]: _________",
        "  Logo filename       [default: logo.gif]            : _________",
        "  CSS filename        [default: styles.css]          : _________",
        "",
        "─" * 60,
        "STEP 4 — Contact Information (optional)",
        "─" * 60,
        "  Contact name   (or leave blank): _____________________________",
        "  Contact email  (or leave blank): _____________________________",
        "",
        "─" * 60,
        "STEP 5 — Folder Paths",
        "─" * 60,
        "  GEDCOM source folder   [default: /Users/jamesrose/GRASP/GRASP_User/gedcoms] : ___",
        "  GEDCOM dest folder     [default: /Users/jamesrose/GRASP/GRASP_User/gedcoms] : ___",
        "  Media source folder    [default: /Users/jamesrose/GRASP/GRASP_User/assets]: ___",
        "  Media dest folder      [default: /Users/jamesrose/GRASP/GRASP_User/assets]: ___",
        "  Styles source folder   [default: /Users/jamesrose/GRASP/GRASP_User/assets]: __",
        "  Styles dest folder     [default: /Users/jamesrose/GRASP/GRASP_User/website/css]: _",
        "  Assets source folder   [default: /Users/jamesrose/GRASP/GRASP_User/assets]: __",
        "  Assets dest folder     [default: /Users/jamesrose/GRASP/GRASP_User/assets]: _",
        "  Special images source  : ______________________________________",
        "  Special images dest    : ______________________________________",
        "  ResultsList.plist src  : ______________________________________",
        "  ResultsList.plist dest : ______________________________________",
        "  Website output folder  [default: /Users/jamesrose/GRASP/GRASP_User/website]     : ___",
        "",
        "─" * 60,
        "  Notes / reminders:",
        "",
        "",
        "",
        "═" * 60,
        "  When ready, run:  python3 setup.py",
        "═" * 60,
        "",
    ]
    print("\n".join(lines))


def print_config_template(output_path="config_template.ini"):
    """Write a commented config.ini template the user can edit directly."""
    template = """\
# ╔══════════════════════════════════════════════════════════╗
# ║       FamilyGenealogy — config_template.ini              ║
# ╚══════════════════════════════════════════════════════════╝
#
# Edit this file in any text editor, then rename it to
# config.ini and place it wherever you like.
#
# Point any script to it with:
#   python3 scripts/gedcom_to_mysql.py --config "/path/to/config.ini"
#
# Lines starting with # are comments and are ignored.
# ──────────────────────────────────────────────────────────

[Database]
# Your MySQL server host (usually localhost)
Host = localhost

# Your MySQL username
User = genealogy

# Your MySQL password — leave blank if you have none
Password = lizrrose

# The name of the MySQL database for your genealogy data
Database = genealogy


[Website]
# Name that appears in the site header on every page
Header_Name = Liz's Genealogy

# Family name used on the welcome / home page
Welcome_Family = Liz's Genealogy

# Copyright line shown in the site footer
Copyright_Info = All rights reserved.

# Logo image filename (must exist in your assets folder)
Logo_File = logo.gif

# CSS stylesheet filename (must exist in your styles folder)
CSS_File = styles.css


[Contact]
# Your name as the site contact (optional — leave blank to omit)
Contact =

# Your email address (optional — leave blank to omit)
Email =


[Paths]
# ── GEDCOM files ──────────────────────────────────────────
# Folder where your original .ged files live
Gedcom_Source = /Users/jamesrose/GRASP/GRASP_User/gedcoms

# Folder where cleaned .ged files are written
Gedcom = /Users/jamesrose/GRASP/GRASP_User/gedcoms

# ── Media (photos, documents) ─────────────────────────────
# Your original photos and documents
Media_Source = /Users/jamesrose/GRASP/GRASP_User/assets

# Folder the website reads media from
Media = /Users/jamesrose/GRASP/GRASP_User/assets

# ── Styles / CSS ──────────────────────────────────────────
Styles_Source = /Users/jamesrose/GRASP/GRASP_User/assets
Styles = /Users/jamesrose/GRASP/GRASP_User/website/css

# ── Assets (logo, Template.jpg, etc.) ────────────────────
Assets_Source = /Users/jamesrose/GRASP/GRASP_User/assets
Assets = /Users/jamesrose/GRASP/GRASP_User/assets

# ── Special images (optional) ────────────────────────────
Special_Images_Source =
Special_Images =

# ── ResultsList.plist (optional) ─────────────────────────
ResultsList_Source =
ResultsList =

# ── Website output ────────────────────────────────────────
# Folder where the generated HTML pages are written
Output_Dir = /Users/jamesrose/GRASP/GRASP_User/website

# ── This config file ──────────────────────────────────────
Config_Source =
Config =
"""
    with open(output_path, "w") as f:
        f.write(template)
    print(f"\n  ✓ Config template written to: {output_path}")
    print()
    print("  Open it in any text editor, fill in your values,")
    print("  then rename it to config.ini and save it wherever you like.")
    print()
    print("  Use it with any script via --config:")
    print('    python3 scripts/gedcom_to_mysql.py --config "/path/to/config.ini"')
    print()


def copy_config(source=None, dest=None):
    """Copy a config template to config.ini."""
    import shutil
    if source is None:
        source = input("  Source (config_template.ini path) [config_template.ini]: ").strip()
        if not source:
            source = "config_template.ini"
    if dest is None:
        dest = input("  Destination (config.ini path) [config.ini]: ").strip()
        if not dest:
            dest = "config.ini"
    source = os.path.expanduser(source)
    dest   = os.path.expanduser(dest)
    if not os.path.isfile(source):
        print(f"  ✗ Source file not found: {source}")
        return
    os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
    shutil.copy2(source, dest)
    print(f"  ✓ Copied: {source}")
    print(f"       to: {dest}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="FamilyGenealogy Setup")
    parser.add_argument(
        "--print-form", action="store_true",
        help="Print a blank paper reference form of all setup questions and exit."
    )
    parser.add_argument(
        "--print-config", metavar="PATH", nargs="?", const="config_template.ini",
        help="Write a commented config.ini template you can edit directly. "
             "Optionally specify output path (default: config_template.ini)."
    )
    parser.add_argument(
        "--copy-config", metavar="SOURCE", nargs="?", const="config_template.ini",
        help="Copy config template to config.ini. Optionally specify source path."
    )
    parser.add_argument(
        "--copy-config-dest", metavar="DEST", default="config.ini",
        help="Destination path for --copy-config (default: config.ini)."
    )
    args = parser.parse_args()

    if args.print_form:
        print_form()
        sys.exit(0)

    if args.print_config is not None:
        print_config_template(args.print_config)
        sys.exit(0)

    if args.copy_config is not None:
        copy_config(source=args.copy_config, dest=args.copy_config_dest)
        sys.exit(0)

    # ── Show header and menu ──────────────────────────────────────────────────
    print(BANNER)
    choice = input(MENU).strip()
    if not choice:
        choice = "1"

    if choice == "2":
        print()
        print_form()
        sys.exit(0)
    elif choice == "3":
        print()
        out = input("  Save template to [config_template.ini]: ").strip()
        if not out:
            out = "config_template.ini"
        print_config_template(out)
        sys.exit(0)
    elif choice == "4":
        print()
        copy_config()
        sys.exit(0)
    elif choice != "1":
        print("  Invalid choice. Starting interactive wizard.")

    print()
    print("  Press Enter to accept any default shown in [brackets].")

    # ── Where to save the config ─────────────────────────────────────────────
    print(DIVIDER)
    print("STEP 1 — Where should your config.ini be saved?")
    print()
    default_config_dir = os.path.expanduser("/Users/jamesrose/GRASP/GRASP_User")
    config_dir = ask_path(f"Config folder (will be created if needed)", must_exist=False)
    if not config_dir:
        config_dir = default_config_dir
    config_dir = os.path.expanduser(config_dir)
    os.makedirs(config_dir, exist_ok=True)
    config_path = os.path.join(config_dir, "config.ini")
    print(f"\n  ✓ Config will be saved to: {config_path}")

    # ── Database ─────────────────────────────────────────────────────────────
    print(DIVIDER)
    print("STEP 2 — MySQL Database Settings")
    print("  These are used by gedcom_to_mysql.py and generate_website.py.")
    print()
    db_host     = ask("MySQL host",          default="localhost")
    db_user     = ask("MySQL user",          default="genealogy")
    db_password = ask("MySQL password",      default="lizrrose", password=True)
    db_name     = ask("MySQL database name", default="genealogy")

    # ── Website ───────────────────────────────────────────────────────────────
    print(DIVIDER)
    print("STEP 3 — Website Settings")
    print("  These appear in the generated HTML pages.")
    print()
    header_name    = ask("Site header name (e.g. The Rose Family)",  default="Liz's Genealogy")
    welcome_family = ask("Welcome page family name",                 default=header_name)
    copyright_info = ask("Copyright line",                           default="All rights reserved.")
    logo_file      = ask("Logo filename",                            default="logo.gif")
    css_file       = ask("CSS filename",                             default="styles.css")

    # ── Contact ───────────────────────────────────────────────────────────────
    print(DIVIDER)
    print("STEP 4 — Contact Information (optional)")
    print()
    contact_name  = ask("Contact name  (or leave blank)")
    contact_email = ask("Contact email (or leave blank)")

    # ── Paths ─────────────────────────────────────────────────────────────────
    print(DIVIDER)
    print("STEP 5 — Folder Paths")
    print("  These tell the scripts where to find and put your files.")
    print("  Leave blank to skip any path you don't use yet.")
    print()

    print("  ── GEDCOM files ──")
    gedcom_source = ask_path("Folder where your .ged files live (source)",         default="/Users/jamesrose/GRASP/GRASP_User/gedcoms")
    gedcom_dest   = ask_path("Folder to copy cleaned .ged files into (destination)", default="/Users/jamesrose/GRASP/GRASP_User/gedcoms")

    print()
    print("  ── Media (photos, documents) ──")
    media_source  = ask_path("Media source folder (your original photos/docs)",    default="/Users/jamesrose/GRASP/GRASP_User/assets")
    media_dest    = ask_path("Media destination folder (where the website reads from)", default="/Users/jamesrose/GRASP/GRASP_User/assets")

    print()
    print("  ── Styles / CSS ──")
    styles_source = ask_path("Styles source folder (where your .css files live)",  default="/Users/jamesrose/GRASP/GRASP_User/assets")
    styles_dest   = ask_path("Styles destination folder",                          default="/Users/jamesrose/GRASP/GRASP_User/website/css")

    print()
    print("  ── Assets (logo, template images) ──")
    assets_source = ask_path("Assets source folder (logo.gif, Template.jpg, etc.)", default="/Users/jamesrose/GRASP/GRASP_User/assets")
    assets_dest   = ask_path("Assets destination folder",                           default="/Users/jamesrose/GRASP/GRASP_User/assets")

    print()
    print("  ── Special images ──")
    special_images_source = ask_path("Special images source folder (optional)")
    special_images_dest   = ask_path("Special images destination folder (optional)")

    print()
    print("  ── ResultsList.plist ──")
    results_list_source = ask("ResultsList.plist source path (full path, optional)")
    results_list_dest   = ask("ResultsList.plist destination path (optional)")

    print()
    print("  ── Website output ──")
    output_dir = ask_path("Website output folder (where HTML is generated)", default="/Users/jamesrose/GRASP/GRASP_User/website")

    # ── Write config ──────────────────────────────────────────────────────────
    cfg = configparser.ConfigParser()

    cfg["Database"] = {
        "Host":     db_host,
        "User":     db_user,
        "Password": db_password,
        "Database": db_name,
    }

    cfg["Website"] = {
        "Header_Name":    header_name,
        "Welcome_Family": welcome_family,
        "Copyright_Info": copyright_info,
        "Logo_File":      logo_file,
        "CSS_File":       css_file,
    }

    cfg["Contact"] = {
        "Contact": contact_name,
        "Email":   contact_email,
    }

    cfg["Paths"] = {
        "Gedcom_Source":         gedcom_source,
        "Gedcom":                gedcom_dest,
        "Media_Source":          media_source,
        "Media":                 media_dest,
        "Styles_Source":         styles_source,
        "Styles":                styles_dest,
        "Assets_Source":         assets_source,
        "Assets":                assets_dest,
        "Special_Images_Source": special_images_source,
        "Special_Images":        special_images_dest,
        "ResultsList_Source":    results_list_source,
        "ResultsList":           results_list_dest,
        "Output_Dir":            output_dir,
        "Config_Source":         config_path,
        "Config":                config_path,
    }

    with open(config_path, "w") as f:
        cfg.write(f)

    # ── Done ──────────────────────────────────────────────────────────────────
    print(DIVIDER)
    print(f"  ✓ Config saved to: {config_path}")
    print()
    print("  Use --config with any script to point it to your config:")
    print()
    print(f"    python3 scripts/gedcom_cleanup.py detect \\")
    print(f"        --config \"{config_path}\" \\")
    print(f"        --input your_file.ged ...")
    print()
    print(f"    python3 scripts/gedcom_to_mysql.py your_file.ged \\")
    print(f"        --config \"{config_path}\"")
    print()
    print(f"    python3 scripts/generate_website.py \\")
    print(f"        --config \"{config_path}\"")
    print()
    print("  To update any setting, just re-run this script.")
    print()

    # ── Reminder about database schema ────────────────────────────────────────
    print("  Before your first import, set up the MySQL database schema:")
    print()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_dir, "..", "database", "schema.sql")
    schema_path = os.path.normpath(schema_path)
    print(f"    mysql -u {db_user} -p {db_name} < \"{schema_path}\"")
    print()
    print("=" * 60)
    print("  Setup complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
