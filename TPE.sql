-- CREACIÓN DE LAS TABLAS
CREATE TABLE futbolista (
    nombre TEXT PRIMARY KEY,
    posicion TEXT NOT NULL,
    edad INT CHECK (edad > 0),
    altura NUMERIC(3, 2) CHECK (altura > 0),
    pie TEXT CHECK (pie IN ('derecho', 'izquierdo','ambidiestro')),
    fichado DATE,
    equipo_anterior TEXT,
    valor_mercado NUMERIC(15, 2) CHECK (valor_mercado >= 0),
    equipo TEXT NOT NULL
);

CREATE TABLE dorsal (
    jugador TEXT PRIMARY KEY REFERENCES futbolista(nombre),
    dorsal INT NOT NULL -- Número asignado
);

-- FUNCIÓN PARA ASIGNAR DORSAL AUTOMÁTICAMENTE
CREATE OR REPLACE FUNCTION asignar_dorsal(nombre_equipo TEXT, posicion TEXT) RETURNS INT AS $$
DECLARE
    numero INT;
BEGIN
    CASE
        WHEN posicion = 'Portero' THEN numero := 1;
        WHEN posicion = 'Defensa' OR posicion = 'Defensa central' THEN numero := 2;
        WHEN posicion = 'Lateral izquierdo' THEN numero := 3;
        WHEN posicion = 'Lateral derecho' THEN numero := 4;
        WHEN posicion = 'Pivote' THEN numero := 5;
        WHEN posicion IN ('Mediocentro', 'Centrocampista', 'Interior derecho', 'Interior izquierdo') THEN numero := 8;
        WHEN posicion IN ('Mediocentro ofensivo', 'Mediapunta') THEN numero := 10;
        WHEN posicion = 'Extremo derecho' THEN numero := 7;
        WHEN posicion = 'Extremo izquierdo' THEN numero := 11;
        WHEN posicion = 'Delantero' OR posicion = 'Delantero centro' THEN numero := 9;
        ELSE numero := 13;
    END CASE;

    IF EXISTS (
        SELECT 1
        FROM dorsal d
        JOIN futbolista f ON d.jugador = f.nombre
        WHERE f.equipo = nombre_equipo AND d.dorsal = numero
    ) THEN
        numero := 13;
        LOOP
            IF NOT EXISTS (
                SELECT 1
                FROM dorsal d
                JOIN futbolista f ON d.jugador = f.nombre
                WHERE f.equipo = nombre_equipo AND d.dorsal = numero
            ) THEN
                EXIT;
            END IF;

            numero := numero + 1;

            IF numero > 99 THEN
                RAISE EXCEPTION 'No hay dorsales disponibles en el rango 13-99 para el equipo %', nombre_equipo;
            END IF;
        END LOOP;
    END IF;

    RETURN numero;
END;
$$ LANGUAGE plpgsql;


-- TRIGGER PARA GARANTIZAR DEPENDENCIA FUNCIONAL EQUIPO DORSAL -> JUGADOR
CREATE OR REPLACE FUNCTION validar_dependencia_funcional() RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM dorsal d
        JOIN futbolista f ON d.jugador = f.nombre
        WHERE f.equipo = (SELECT equipo FROM futbolista WHERE nombre = NEW.jugador)
          AND d.dorsal = NEW.dorsal
          AND f.nombre != NEW.jugador
    ) THEN
        RAISE EXCEPTION 'El dorsal % ya está asignado a otro jugador en el equipo %', NEW.dorsal, (SELECT equipo FROM futbolista WHERE nombre = NEW.jugador);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER trg_validar_dependencia_funcional
BEFORE INSERT ON dorsal
FOR EACH ROW
EXECUTE FUNCTION validar_dependencia_funcional();

-- IMPORTACIÓN DE DATOS DESDE CSV
SET datestyle = 'DMY';
-- IMPORTACIÓN DE DATOS DESDE CSV
COPY futbolista(nombre, posicion, edad, altura, pie, fichado, equipo_anterior, valor_mercado, equipo)
FROM '/tmp/jugadores-2022.csv'
DELIMITER ';' CSV HEADER;

-- ASIGNACIÓN AUTOMÁTICA DE DORSALES
INSERT INTO dorsal(jugador, dorsal)
SELECT nombre, asignar_dorsal(equipo, posicion) FROM futbolista;

-- FUNCIÓN PARA ANÁLISIS DE JUGADORES Y EQUIPOS
DROP FUNCTION IF EXISTS analisis_jugadores(DATE);

CREATE OR REPLACE FUNCTION analisis_jugadores(dia DATE)
RETURNS TABLE (
    Variable TEXT,
    Fecha DATE,
    Qty INT,
    Prom_Edad NUMERIC,
    Prom_Alt NUMERIC,
    Valor NUMERIC,
    linea INT
) AS $$
BEGIN
    -- Análisis por pies
    RETURN QUERY
    SELECT
        'Pie ' || pie AS Variable,
        DATE_TRUNC('month', fichado)::DATE AS Fecha,
        COUNT(*)::INT AS Qty,
        AVG(edad) AS Prom_Edad,
        AVG(altura) AS Prom_Alt,
        MAX(valor_mercado) AS Valor,
        ROW_NUMBER() OVER (PARTITION BY pie ORDER BY DATE_TRUNC('month', fichado)::DATE)::INT AS linea
    FROM futbolista
    WHERE fichado > dia
    GROUP BY pie, DATE_TRUNC('month', fichado)
    ORDER BY pie, Fecha;

    -- Análisis por equipos
    RETURN QUERY
    SELECT
        'Equipo ' || equipo AS Variable,
        MIN(fichado)::DATE AS Fecha, -- Conversión explícita a DATE
        COUNT(*)::INT AS Qty, -- Conversión a INT
        AVG(edad) AS Prom_Edad,
        AVG(altura) AS Prom_Alt,
        MAX(valor_mercado) AS Valor,
        ROW_NUMBER() OVER (ORDER BY MAX(valor_mercado) DESC)::INT AS linea -- Conversión explícita a INT
    FROM futbolista
    WHERE fichado > dia
    GROUP BY equipo
    ORDER BY Valor DESC;

    -- Análisis por dorsales principales
    RETURN QUERY
    SELECT
        'Dorsal:' || dorsal.dorsal AS Variable,
        MIN(futbolista.fichado)::DATE AS Fecha,
        COUNT(*)::INT AS Qty,
        AVG(futbolista.edad) AS Prom_Edad,
        AVG(futbolista.altura) FILTER (WHERE futbolista.altura IS NOT NULL) AS Prom_Alt,
        MAX(futbolista.valor_mercado) FILTER (WHERE futbolista.valor_mercado IS NOT NULL) AS Valor,
        ROW_NUMBER() OVER (ORDER BY MAX(futbolista.valor_mercado) DESC)::INT AS linea
    FROM futbolista
    JOIN dorsal ON futbolista.nombre = dorsal.jugador
    WHERE futbolista.fichado IS NOT NULL
    AND futbolista.fichado > dia AND dorsal.dorsal < 13
    GROUP BY dorsal.dorsal
    ORDER BY Valor DESC;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM analisis_jugadores('2022-07-22');



