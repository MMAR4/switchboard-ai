from switchboard_core1 import SwitchboardEngine

# Initialize engine once (sets default threshold to 1.0 GB)
engine = SwitchboardEngine(threshold_gb=1.0)

print("\n" + "="*60)
print(" 🔀 SWITCHBOARD AI - INTERACTIVE SQL TERMINAL")
print(" Type 'exit' or 'quit' to close.")
print("="*60 + "\n")

while True:
    try:
        user_sql = input("\n[Switchboard] Enter SQL Query > ")
        
        if user_sql.strip().lower() in ["exit", "quit", "q"]:
            print("Exiting Switchboard CLI. Goodbye!")
            break
            
        if not user_sql.strip():
            continue

        # Execute through the engine instance
        df = engine.execute(user_sql)
        print("\n--- QUERY RESULT ---")
        print(df)

    except KeyboardInterrupt:
        print("\nExiting...")
        break
    except Exception as e:
        print(f"\n❌ Execution Error: {e}")