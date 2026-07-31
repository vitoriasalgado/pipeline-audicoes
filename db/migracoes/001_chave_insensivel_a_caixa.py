"""001 — chave de negócio insensível a caixa (jul/2026).

O `db/schema.sql` descreve o estado final do warehouse, mas o Postgres só aplica
aquele arquivo quando o volume de dados está vazio. Num warehouse já povoado, a
mudança precisa vir por aqui.

O que faz:
  1. funde linhas de dim_artista / dim_faixa que diferem só na caixa das letras
     ('Zayn'/'ZAYN'), mantendo a variante com mais scrobbles e repontando as FKs;
  2. cria os índices únicos por lower(nome), que os upserts das DAGs usam no
     ON CONFLICT.

A ordem importa. Sem a fusão, o CREATE INDEX falha com "Key (lower(nome))=(zayn)
is duplicated". E fundir artistas pode criar colisão nova em dim_faixa (duas
faixas de mesmo nome passam a dividir o mesmo artista_id), então isso é resolvido
durante a fusão dos artistas, antes de fundir as faixas — sempre fundindo a
homônima primeiro e só depois repontando, senão o UNIQUE (nome, artista_id) que
ainda existe em dim_faixa é violado no meio do caminho.

**Dois critérios de sobrevivência, para contextos diferentes.** Aqui, a variante
que fica é a de **mais scrobbles** (empate → menor id): é uma decisão histórica,
sobre qual grafia representa melhor o que foi de fato ouvido. Já em tempo de
execução, o `ON CONFLICT ... DO UPDATE` das DAGs nunca altera a coluna `nome`,
então vale a regra oposta: **a primeira grafia vista é a que fica**. Uma
migração reorganiza o passado; o upsert preserva o presente.

Rodar no host, a partir da raiz do projeto, com a venv ativa:

    python db/migracoes/001_chave_insensivel_a_caixa.py

Seguro de rodar de novo: sem duplicatas não faz nada, e os índices são IF NOT EXISTS.
"""

import psycopg2

conn = psycopg2.connect(host="localhost", port=5433, dbname="warehouse",
                        user="warehouse", password="warehouse")
cur = conn.cursor()


def fundir_faixa(perdedor, sobrevivente):
    cur.execute(
        """
        UPDATE dim_faixa d SET
            spotify_track_id    = COALESCE(d.spotify_track_id, p.spotify_track_id),
            album               = COALESCE(d.album, p.album),
            na_biblioteca       = COALESCE(d.na_biblioteca, p.na_biblioteca),
            biblioteca_added_at = COALESCE(d.biblioteca_added_at, p.biblioteca_added_at)
        FROM dim_faixa p WHERE d.id = %s AND p.id = %s;
        """,
        (sobrevivente, perdedor),
    )
    # UNIQUE (scrobble_uts, faixa_id) colidiria no repontamento
    cur.execute(
        """
        DELETE FROM fato_audicoes f
         WHERE f.faixa_id = %s
           AND EXISTS (SELECT 1 FROM fato_audicoes g
                        WHERE g.faixa_id = %s AND g.scrobble_uts = f.scrobble_uts);
        """,
        (perdedor, sobrevivente),
    )
    cur.execute("UPDATE fato_audicoes SET faixa_id = %s WHERE faixa_id = %s;",
                (sobrevivente, perdedor))
    cur.execute("UPDATE fato_top_spotify SET faixa_id = %s WHERE faixa_id = %s;",
                (sobrevivente, perdedor))
    cur.execute("DELETE FROM dim_faixa WHERE id = %s;", (perdedor,))


def por_scrobbles(ids):
    cur.execute(
        """
        SELECT f.id, count(fa.id) FROM dim_faixa f
        LEFT JOIN fato_audicoes fa ON fa.faixa_id = f.id
        WHERE f.id = ANY(%s) GROUP BY f.id ORDER BY count(fa.id) DESC, f.id;
        """,
        (ids,),
    )
    return cur.fetchall()


cur.execute("SELECT lower(nome), array_agg(id) FROM dim_artista GROUP BY 1 HAVING count(*) > 1;")
grupos = cur.fetchall()
print(f"{len(grupos)} grupos de artista para fundir", flush=True)

for _, ids in grupos:
    cur.execute(
        """
        SELECT a.id, a.nome, count(fa.id) FROM dim_artista a
        LEFT JOIN dim_faixa f ON f.artista_id = a.id
        LEFT JOIN fato_audicoes fa ON fa.faixa_id = f.id
        WHERE a.id = ANY(%s) GROUP BY a.id, a.nome ORDER BY count(fa.id) DESC, a.id;
        """,
        (ids,),
    )
    ranked = cur.fetchall()
    sobrevivente, nome, n = ranked[0]
    perdedores = [r[0] for r in ranked[1:]]
    print(f"  {nome!r} ({n} scrobbles) <- {[r[1] for r in ranked[1:]]}", flush=True)

    cur.execute(
        """
        UPDATE dim_artista SET spotify_artist_id = COALESCE(spotify_artist_id,
            (SELECT spotify_artist_id FROM dim_artista
              WHERE id = ANY(%s) AND spotify_artist_id IS NOT NULL LIMIT 1))
        WHERE id = %s;
        """,
        (perdedores, sobrevivente),
    )
    cur.execute("UPDATE fato_top_spotify SET artista_id = %s WHERE artista_id = ANY(%s);",
                (sobrevivente, perdedores))

    cur.execute("SELECT id, nome FROM dim_faixa WHERE artista_id = ANY(%s);", (perdedores,))
    for fid, fnome in cur.fetchall():
        cur.execute("SELECT id FROM dim_faixa WHERE artista_id = %s AND nome = %s;",
                    (sobrevivente, fnome))
        existente = cur.fetchone()
        if existente:
            manter, sair = [r[0] for r in por_scrobbles([fid, existente[0]])]
            if manter == fid:
                # funde ANTES de repontar: enquanto a homônima do sobrevivente
                # existir, o UPDATE viola o UNIQUE (nome, artista_id)
                fundir_faixa(sair, fid)
                cur.execute("UPDATE dim_faixa SET artista_id = %s WHERE id = %s;",
                            (sobrevivente, fid))
            else:
                fundir_faixa(fid, manter)
        else:
            cur.execute("UPDATE dim_faixa SET artista_id = %s WHERE id = %s;",
                        (sobrevivente, fid))

    cur.execute("DELETE FROM dim_artista WHERE id = ANY(%s);", (perdedores,))

cur.execute("SELECT artista_id, lower(nome), array_agg(id) FROM dim_faixa GROUP BY 1,2 HAVING count(*) > 1;")
grupos = cur.fetchall()
print(f"{len(grupos)} grupos de faixa para fundir", flush=True)

for _, _, ids in grupos:
    ranked = por_scrobbles(ids)
    for perdedor, _ in ranked[1:]:
        fundir_faixa(perdedor, ranked[0][0])

cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS dim_artista_nome_lower_uq "
            "ON dim_artista (lower(nome));")
cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS dim_faixa_nome_lower_artista_uq "
            "ON dim_faixa (lower(nome), artista_id);")

conn.commit()

cur.execute("SELECT count(*) FROM fato_audicoes;")
print(f"indices criados. fato_audicoes com {cur.fetchone()[0]} linhas.", flush=True)
cur.close()
conn.close()
