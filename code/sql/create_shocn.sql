-- Table: public.shocn

-- DROP TABLE public.shocn;

CREATE TABLE public.shocn
(
    uuid text COLLATE pg_catalog."default" NOT NULL,
    identifier text COLLATE pg_catalog."default",
    summary text COLLATE pg_catalog."default",
    producttype text COLLATE pg_catalog."default",
    filename text COLLATE pg_catalog."default",
    size text COLLATE pg_catalog."default",
    grd_uuid text COLLATE pg_catalog."default",
    CONSTRAINT shocn_pkey PRIMARY KEY (uuid)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.shocn
    OWNER to postgres;