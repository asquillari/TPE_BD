--Creación de Tablas--
CREATE TABLE futbolista (
    nombre TEXT PRIMARY KEY,
    posicion TEXT NOT NULL CHECK (posicion IN (
        'Portero', 'Defensa', 'Defensa central', 'Lateral izquierdo',
        'Lateral derecho', 'Pivote', 'Mediocentro', 'Centrocampista',
        'Interior derecho', 'Interior izquierdo', 'Mediocentro ofensivo',
        'Mediapunta', 'Extremo derecho', 'Extremo izquierdo',
        'Delantero', 'Delantero centro'
    )),
    edad INT NOT NULL CHECK (edad > 0 AND edad < 100),
    altura NUMERIC(3, 2) CHECK (altura > 0 AND altura < 3),
    pie TEXT CHECK (pie IN ('derecho', 'izquierdo', 'ambidiestro')),
    fichado DATE,
    equipo_anterior TEXT,
    valor_mercado NUMERIC(15, 2) CHECK (valor_mercado IS NULL OR valor_mercado >= 0),
    equipo TEXT
);

CREATE TABLE dorsal (
    jugador TEXT PRIMARY KEY REFERENCES futbolista(nombre),
    dorsal INT NOT NULL CHECK (dorsal BETWEEN 1 AND 99)
);

-- Función para asignar dorsal
CREATE OR REPLACE FUNCTION asignar_dorsal()
RETURNS TRIGGER AS $$
DECLARE
    preferred_dorsals INT[];
    assigned_dorsal INT := NULL;
    dorsal_candidate INT;
    equipo_actual TEXT;
BEGIN
    SELECT equipo INTO equipo_actual
    FROM futbolista
    WHERE nombre = NEW.nombre;

    CASE TRIM(LOWER(NEW.posicion))
        WHEN 'portero' THEN
            preferred_dorsals := ARRAY[1, 12];
        WHEN 'defensa', 'defensa central' THEN
            preferred_dorsals := ARRAY[2, 6];
        WHEN 'lateral izquierdo' THEN
            preferred_dorsals := ARRAY[3];
        WHEN 'lateral derecho' THEN
            preferred_dorsals := ARRAY[4];
        WHEN 'pivote' THEN
            preferred_dorsals := ARRAY[5];
        WHEN 'mediocentro', 'centrocampista', 'interior derecho', 'interior izquierdo' THEN
            preferred_dorsals := ARRAY[8];
        WHEN 'mediocentro ofensivo', 'mediapunta' THEN
            preferred_dorsals := ARRAY[10];
        WHEN 'extremo derecho' THEN
            preferred_dorsals := ARRAY[7, 11];
        WHEN 'extremo izquierdo' THEN
            preferred_dorsals := ARRAY[7, 11];
        WHEN 'delantero', 'delantero centro' THEN
            preferred_dorsals := ARRAY[9];
        ELSE
            preferred_dorsals := ARRAY[]::INT[];
    END CASE;

    assigned_dorsal := NULL;
    FOREACH dorsal_candidate IN ARRAY preferred_dorsals LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM dorsal d
            JOIN futbolista f ON d.jugador = f.nombre
            WHERE f.equipo = equipo_actual AND d.dorsal = dorsal_candidate
        ) THEN
            assigned_dorsal := dorsal_candidate;
            EXIT;
        END IF;
    END LOOP;

    IF assigned_dorsal IS NULL THEN
        FOR dorsal_candidate IN 13..99 LOOP
            IF NOT EXISTS (
                SELECT 1
                FROM dorsal d
                JOIN futbolista f ON d.jugador = f.nombre
                WHERE f.equipo = equipo_actual AND d.dorsal = dorsal_candidate
            ) THEN
                assigned_dorsal := dorsal_candidate;
                EXIT;
            END IF;
        END LOOP;
    END IF;

    IF assigned_dorsal IS NULL THEN
        RAISE EXCEPTION 'No hay dorsales disponibles para el jugador % en el equipo %', NEW.nombre, equipo_actual;
    END IF;

    INSERT INTO dorsal (jugador, dorsal)
    VALUES (NEW.nombre, assigned_dorsal);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Creación del trigger para asignar dorsal automáticamente después de insertar en futbolista
DROP TRIGGER IF EXISTS asignar_dorsal_trigger ON futbolista;

CREATE TRIGGER asignar_dorsal_trigger
AFTER INSERT ON futbolista
FOR EACH ROW
EXECUTE FUNCTION asignar_dorsal();

--Validación Dependecias Funcionales--
CREATE OR REPLACE FUNCTION validar_dependencia_funcional() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM dorsal d
        JOIN futbolista f ON d.jugador = f.nombre
        WHERE f.equipo = (SELECT equipo FROM futbolista WHERE nombre = NEW.jugador)
          AND d.dorsal = NEW.dorsal
          AND f.nombre != NEW.jugador
    ) THEN
        RAISE EXCEPTION 'El dorsal % ya está asignado a otro jugador en el equipo %',
            NEW.dorsal, (SELECT equipo FROM futbolista WHERE nombre = NEW.jugador);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validar_dependencia_funcional
BEFORE INSERT ON dorsal
FOR EACH ROW
EXECUTE FUNCTION validar_dependencia_funcional();

--Extracción de información del CSV--
SET datestyle TO 'DMY';

COPY futbolista(nombre, posicion, edad, altura, pie, fichado, equipo_anterior, valor_mercado, equipo)
FROM 'jugadores-2022.csv'
DELIMITER ';' CSV HEADER;

--Analisis de Jugadores--
DROP FUNCTION IF EXISTS analisis_jugadores(DATE);

CREATE OR REPLACE FUNCTION analisis_jugadores(dia DATE)
RETURNS VOID AS $$
DECLARE
    v_fecha_min DATE;
    v_pie TEXT;
    v_qty INT;
    v_prom_edad NUMERIC(5, 1);
    v_prom_alt NUMERIC(5, 2);
    v_max_valor NUMERIC(15);
    v_equipo TEXT;
    v_dorsal INT;

    v_line_num INT := 0;
    v_pie_anterior TEXT := NULL;
    v_output TEXT;

    v_var_width TEXT := '35';
    v_date_width TEXT := '15';
    v_qty_width TEXT := '5';
    v_edad_width TEXT := '8';
    v_alt_width TEXT := '8';
    v_valor_width TEXT := '10';
    v_num_width TEXT := '3';

BEGIN
    RAISE NOTICE 'INFO: -------------------------------------------------------------------------------------------';
    RAISE NOTICE 'INFO: -----------------------------ANALISIS DE JUGADORES Y EQUIPOS ------------------------------';
    RAISE NOTICE 'INFO: -------------------------------------------------------------------------------------------';
    RAISE NOTICE 'INFO: -------------------------------------------------------------------------------------------';
    RAISE NOTICE 'INFO: Variable------------------------------Fecha----------Qty--Prom_Edad--Prom_Alt--Valor-----#-';
    RAISE NOTICE 'INFO: -------------------------------------------------------------------------------------------';

    FOR v_fecha_min, v_pie, v_qty, v_prom_edad, v_prom_alt, v_max_valor IN
        SELECT
            TO_DATE(TO_CHAR(fichado, 'YYYY-MM-DD'), 'YYYY-MM') AS mes_fichaje,
            pie,
            COUNT(*) AS qty,
            ROUND(AVG(edad), 1) AS prom_edad,
            ROUND(AVG(altura), 2) AS prom_alt,
            ROUND(MAX(valor_mercado), 0) AS max_valor
        FROM futbolista
        WHERE fichado >= TO_DATE(TO_CHAR(dia, 'YYYY-MM-DD'), 'YYYY-MM-DD')
          AND pie IS NOT NULL
          AND valor_mercado IS NOT NULL
          AND edad IS NOT NULL
          AND altura IS NOT NULL
        GROUP BY mes_fichaje, pie
        ORDER BY pie, mes_fichaje
    LOOP
        IF v_pie <> v_pie_anterior THEN
            v_line_num := 1;
        ELSE
            v_line_num := v_line_num + 1;
        END IF;

        v_output := format('INFO:   %-'||v_var_width||'s %-'||v_date_width||'s %-'||v_qty_width||'s %-'||v_edad_width||'s %-'||v_alt_width||'s %-'||v_valor_width||'s %-'||v_num_width||'s',
            RPAD('Pie: ' || v_pie, v_var_width::INT, '.'),
            TO_CHAR(v_fecha_min, 'YYYY-MM'),
            v_qty::TEXT,
            v_prom_edad::TEXT,
            v_prom_alt::TEXT,
            v_max_valor::TEXT,
            v_line_num::TEXT);

        RAISE NOTICE '%', v_output;
        v_pie_anterior := v_pie;
    END LOOP;

    RAISE NOTICE 'INFO:  ------------------------------------------------------------------------------------------';
    v_line_num := 0;
    FOR v_equipo, v_fecha_min, v_qty, v_prom_edad, v_prom_alt, v_max_valor IN
        SELECT
            equipo,
            TO_DATE(TO_CHAR(MIN(fichado), 'YYYY-MM-DD'), 'YYYY-MM-DD') AS fecha_min,
            COUNT(*) AS qty,
            ROUND(AVG(edad), 1) AS prom_edad,
            ROUND(AVG(altura), 2) AS prom_alt,
            ROUND(MAX(valor_mercado), 0) AS max_valor
        FROM futbolista
        WHERE fichado >= TO_DATE(TO_CHAR(dia, 'YYYY-MM-DD'), 'YYYY-MM-DD')
          AND equipo IS NOT NULL
          AND valor_mercado IS NOT NULL
          AND edad IS NOT NULL
          AND altura IS NOT NULL
        GROUP BY equipo
        ORDER BY max_valor DESC
    LOOP
        v_line_num:= v_line_num + 1;
        v_output := format('INFO:   %-'||v_var_width||'s %-'||v_date_width||'s %-'||v_qty_width||'s %-'||v_edad_width||'s %-'||v_alt_width||'s %-'||v_valor_width||'s %-'||v_num_width||'s',
            RPAD(v_equipo, v_var_width::INT, '.'),
            TO_CHAR(v_fecha_min, 'YYYY-MM-DD'),
            v_qty::TEXT,
            v_prom_edad::TEXT,
            v_prom_alt::TEXT,
            v_max_valor::TEXT,
            v_line_num::TEXT);

        RAISE NOTICE '%', v_output;
    END LOOP;

    RAISE NOTICE 'INFO:  ------------------------------------------------------------------------------------------';
    v_line_num := 0;
    FOR v_dorsal, v_fecha_min, v_qty, v_prom_edad, v_prom_alt, v_max_valor IN
        SELECT
            d.dorsal,
            TO_DATE(TO_CHAR(MIN(f.fichado), 'YYYY-MM-DD'), 'YYYY-MM-DD') AS fecha_min,
            COUNT(*) AS qty,
            ROUND(AVG(f.edad), 1) AS prom_edad,
            ROUND(AVG(f.altura), 2) AS prom_alt,
            ROUND(MAX(f.valor_mercado), 0) AS max_valor
        FROM futbolista f
        JOIN dorsal d ON f.nombre = d.jugador
        WHERE f.fichado >= TO_DATE(TO_CHAR(dia, 'YYYY-MM-DD'), 'YYYY-MM-DD')
          AND f.valor_mercado IS NOT NULL
          AND f.edad IS NOT NULL
          AND f.altura IS NOT NULL
          AND d.dorsal < 13
        GROUP BY d.dorsal
        ORDER BY max_valor DESC
    LOOP
        v_line_num := v_line_num + 1;
        v_output := format('INFO:   %-'||v_var_width||'s %-'||v_date_width||'s %-'||v_qty_width||'s %-'||v_edad_width||'s %-'||v_alt_width||'s %-'||v_valor_width||'s %-'||v_num_width||'s',
            RPAD('Dorsal: ' || v_dorsal::TEXT, v_var_width::INT, '.'),
            TO_CHAR(v_fecha_min, 'YYYY-MM-DD'),
            v_qty::TEXT,
            v_prom_edad::TEXT,
            v_prom_alt::TEXT,
            v_max_valor::TEXT,
            v_line_num);

        RAISE NOTICE '%', v_output;
    END LOOP;

    RAISE NOTICE 'INFO:  ------------------------------------------------------------------------------------------';
END;
$$ LANGUAGE plpgsql;

SELECT  analisis_jugadores('22/07/2022')