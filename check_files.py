import os

print("--- CURRENT WORKING DIRECTORY ---")
cwd = os.getcwd()
print(f"📂 {cwd}")

print("\n--- FILES IN THIS FOLDER ---")
files = os.listdir(cwd)
for f in files:
    print(f"  📄 {f}")

print("\n--- SEARCHING FOR 'staging_tables.sql' ---")
found_sql = False
for root, dirs, files in os.walk(cwd):
    if 'staging_tables.sql' in files:
        print(f"  ✅ FOUND IT HERE: {os.path.join(root, 'staging_tables.sql')}")
        found_sql = True
        
if not found_sql:
    print("  ❌ NOT FOUND. You need to move 'staging_tables.sql' into this folder.")

print("\n--- SEARCHING FOR 'Project Dataset' ---")
if os.path.exists(os.path.join(cwd, 'Project Dataset')):
    print("  ✅ 'Project Dataset' folder exists.")
    # Check inside
    print("     Contents of Project Dataset:")
    try:
        subfolders = os.listdir(os.path.join(cwd, 'Project Dataset'))
        for sub in subfolders:
            print(f"       - {sub}")
    except:
        pass
else:
    print("  ❌ 'Project Dataset' folder is MISSING.")