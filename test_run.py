from dbt_sage.parser import DbtManifestParser

def test_parser():
    print("Testing DbtManifestParser with existing project...")
    project_path = r"c:\Users\Orb\Desktop\Проекти гитхаб\defi-revenue-attribution\dbt_project"
    
    parser = DbtManifestParser(project_path)
    loaded = parser.load()
    print(f"Manifest loaded: {loaded}")
    
    if loaded:
        models = parser.get_models()
        print(f"\nFound {len(models)} models:")
        for m in models[:3]:
            print(f" - {m['name']} ({m['materialized']})")
            
        print("\nTesting get_model_details for 'stg_dune__wallet_labels':")
        details = parser.get_model_details("stg_dune__wallet_labels")
        if details:
            print(f"Path: {details.get('path')}")
            print(f"Columns: {list(details.get('columns', {}).keys())}")
            print(f"Depends on: {details.get('depends_on')}")

if __name__ == "__main__":
    test_parser()
