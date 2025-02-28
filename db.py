def init_db():
    """
    Crea las tablas necesarias si no existen y los índices.
    También verifica y actualiza la estructura de las tablas existentes.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        
        # Primero verificar si las tablas existen y tienen la estructura correcta
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'transactions')")
        transactions_exists = cur.fetchone()[0]
        
        # Si la tabla de transacciones existe, verificar si tiene la columna 'token'
        if transactions_exists:
            cur.execute("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'token')")
            token_column_exists = cur.fetchone()[0]
            
            # Si no existe la columna 'token', agregarla
            if not token_column_exists:
                print("⚠️ La tabla 'transactions' existe pero no tiene columna 'token'. Agregando columna...")
                cur.execute("ALTER TABLE transactions ADD COLUMN token TEXT")
                conn.commit()
                print("✅ Columna 'token' agregada a la tabla 'transactions'")
        
        # Tabla de puntuaciones de wallets
        cur.execute("""
            CREATE TABLE IF NOT EXISTS wallet_scores (
                wallet TEXT PRIMARY KEY,
                score NUMERIC,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Tabla de transacciones
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                token TEXT,
                tx_type TEXT,
                amount_usd NUMERIC,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Tabla de señales
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                token TEXT,
                trader_count INTEGER,
                confidence NUMERIC,
                initial_price NUMERIC,
                created_at TIMESTAMP DEFAULT NOW(),
                outcome_collected BOOLEAN DEFAULT FALSE
            )
        """)
        
        # Tabla de rendimiento de señales
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_performance (
                id SERIAL PRIMARY KEY,
                token TEXT,
                signal_id INTEGER REFERENCES signals(id),
                timeframe TEXT,
                percent_change NUMERIC,
                confidence NUMERIC,
                traders_count INTEGER,
                timestamp TIMESTAMP DEFAULT NOW(),
                UNIQUE(token, timeframe)
            )
        """)
        
        # Tabla de configuración del bot
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Insertar configuraciones iniciales
        default_settings = [
            ("min_transaction_usd", str(Config.MIN_TRANSACTION_USD)),
            ("min_traders_for_signal", str(Config.MIN_TRADERS_FOR_SIGNAL)),
            ("signal_window_seconds", str(Config.SIGNAL_WINDOW_SECONDS)),
            ("min_confidence_threshold", str(Config.MIN_CONFIDENCE_THRESHOLD)),
            ("rugcheck_min_score", "50"),
            ("min_volume_usd", str(Config.MIN_VOLUME_USD))
        ]
        
        for key, value in default_settings:
            cur.execute("""
                INSERT INTO bot_settings (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO NOTHING
            """, (key, value))
        
        # Verificar que la columna 'token' existe antes de crear el índice
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'token')")
        token_column_exists = cur.fetchone()[0]
        
        # Crear índices solo si la columna existe
        if token_column_exists:
            # Crear índices para mejorar rendimiento
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_token ON transactions(token)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_created_at ON signals(created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_signals_token ON signals(token)")
        else:
            print("⚠️ No se pudieron crear índices en la tabla 'transactions' porque falta la columna 'token'")

        conn.commit()
        
    print("✅ Base de datos inicializada correctamente")
    return True
