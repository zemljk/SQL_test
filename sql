-- 1) Создадим последовательность для ID договора
DROP SEQUENCE IF EXISTS seq_contracts;
CREATE SEQUENCE seq_contracts START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

-- 2) Создадим таблицу CONTRACTS

CREATE TABLE contracts
(
    contract_id    BIGINT NOT NULL,
    contract_date  DATE,
    phone_number   VARCHAR(20),
    created_at     TIMESTAMP,        -- Дата создания
    created_by     VARCHAR(100),     -- Пользователь, создавший запись
    updated_at     TIMESTAMP,        -- Дата обновления
    updated_by     VARCHAR(100),     -- Пользователь, обновивший запись

    CONSTRAINT pk_contracts PRIMARY KEY (contract_id)
);

-- 3) Функция-триггер для заполнения contract_id и служебных полей (created/updated)
CREATE OR REPLACE FUNCTION contracts_bi()
RETURNS TRIGGER AS
$$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Автогенерация contract_id, если не задано вручную
        IF NEW.contract_id IS NULL THEN
            NEW.contract_id := nextval('seq_contracts');
        END IF;

        NEW.created_at := NOW();
        NEW.created_by := current_user;

        -- При вставке сразу же заполняем updated_at / updated_by
        NEW.updated_at := NOW();
        NEW.updated_by := current_user;

    ELSIF TG_OP = 'UPDATE' THEN
        -- При обновлении фиксируем только updated_*
        NEW.updated_at := NOW();
        NEW.updated_by := current_user;
    END IF;

    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

-- 4) Триггер, который вешаем на таблицу CONTRACTS
CREATE TRIGGER tr_contracts_bi
BEFORE INSERT OR UPDATE
ON contracts
FOR EACH ROW
EXECUTE FUNCTION contracts_bi();


-- 1) Создаём последовательность для комментариев
DROP SEQUENCE IF EXISTS seq_contract_comment;
CREATE SEQUENCE seq_contract_comment START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;

-- 2) Создаём таблицу CONTRACT_COMMENT
CREATE TABLE contract_comment
(
    comment_id    BIGINT NOT NULL,            -- Уникальный ключ комментария
    contract_id   BIGINT NOT NULL,            -- Ссылка на договор
    comment_text  TEXT,                       -- Текст комментария

    created_at    TIMESTAMP,                  -- Дата создания записи
    created_by    VARCHAR(100),               -- Пользователь, создавший запись
    updated_at    TIMESTAMP,                  -- Дата обновления
    updated_by    VARCHAR(100),               -- Пользователь, обновивший запись

    CONSTRAINT pk_contract_comment PRIMARY KEY (comment_id),
    CONSTRAINT fk_contract_comment_contract_id FOREIGN KEY (contract_id)
        REFERENCES contracts (contract_id)
        ON DELETE CASCADE
);

-- 3) Функция-триггер для заполнения comment_id и служебных полей (created/updated)
CREATE OR REPLACE FUNCTION contract_comment_bi()
RETURNS TRIGGER AS
$$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.comment_id IS NULL THEN
            NEW.comment_id := nextval('seq_contract_comment');
        END IF;

        NEW.created_at := NOW();
        NEW.created_by := current_user;

        -- При вставке сразу же заполняем updated_at / updated_by
        NEW.updated_at := NOW();
        NEW.updated_by := current_user;

    ELSIF TG_OP = 'UPDATE' THEN
        NEW.updated_at := NOW();
        NEW.updated_by := current_user;
    END IF;

    RETURN NEW;
END;
$$
LANGUAGE plpgsql;

-- 4) Триггер, который вешаем на таблицу CONTRACT_COMMENT
CREATE TRIGGER tr_contract_comment_bi
BEFORE INSERT OR UPDATE
ON contract_comment
FOR EACH ROW
EXECUTE FUNCTION contract_comment_bi();


CREATE OR REPLACE VIEW vw_comments_on_numbers AS
SELECT
    c.phone_number,
    c.contract_date,
    cc.comment_text
FROM contracts c
JOIN contract_comment cc ON c.contract_id = cc.contract_id;


CREATE OR REPLACE VIEW vw_comments_created_this_week AS
SELECT
    c.phone_number,
    c.contract_date,
    cc.comment_text
FROM contracts c
JOIN contract_comment cc ON c.contract_id = cc.contract_id
WHERE c.created_at >= date_trunc('week', current_timestamp);


CREATE OR REPLACE VIEW vw_comments_count_per_contract AS
SELECT
    c.contract_id,
    COUNT(cc.comment_id) AS comment_count
FROM contracts c
LEFT JOIN contract_comment cc ON c.contract_id = cc.contract_id
GROUP BY c.contract_id;


------------------------------------------------------------------------------
-- Функция добавления комментария
-- Входные данные: ID договора, текст комментария
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION add_comment(
    p_contract_id    BIGINT,
    p_comment_text   TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO contract_comment (contract_id, comment_text)
    VALUES (p_contract_id, p_comment_text);
END;
$$;

------------------------------------------------------------------------------
-- Функция обновления комментариев по договору
-- Входные данные: ID договора, текст комментария
-- ПРИМЕЧАНИЕ: без комментария-комментария (comment_id) мы обновим все строки,
-- принадлежащие этому договору. Если нужна более тонкая логика, её надо уточнять.
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION update_comment(
    p_contract_id    BIGINT,
    p_comment_text   TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
    UPDATE contract_comment
    SET comment_text = p_comment_text
    WHERE contract_id = p_contract_id;
END;
$$;

------------------------------------------------------------------------------
-- Функция удаления комментариев по договору
-- Входные данные: ID договора
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION delete_comment(
    p_contract_id BIGINT
)
RETURNS VOID
LANGUAGE plpgsql
AS
$$
BEGIN
    DELETE FROM contract_comment
    WHERE contract_id = p_contract_id;
END;
$$;

------------------------------------------------------------------------------
-- Функция, возвращающая количество договоров,
-- по которым создано 2 и более комментария за текущий период
-- "Текущий период" интерпретируем как "текущий месяц".
------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_contracts_with_2plus_comments_current_period()
RETURNS INTEGER
LANGUAGE plpgsql
AS
$$
DECLARE
    v_count INTEGER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM (
        SELECT contract_id
        FROM contract_comment
        WHERE created_at >= date_trunc('month', current_timestamp)
        GROUP BY contract_id
        HAVING COUNT(*) >= 2
    ) sub;

    RETURN v_count;
END;
$$;
