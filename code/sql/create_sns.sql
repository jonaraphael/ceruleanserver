-- Table: public.sns

-- DROP TABLE public.sns;

CREATE TABLE public.sns
(
    sns_messageid text COLLATE pg_catalog."default" NOT NULL,
    sns_subject text COLLATE pg_catalog."default",
    sns_timestamp numeric,
    grd_id text COLLATE pg_catalog."default",
    grd_uuid text COLLATE pg_catalog."default",
    absoluteorbitnumber numeric,
    footprint geometry,
    mode text COLLATE pg_catalog."default",
    polarization text COLLATE pg_catalog."default",
    s3ingestion numeric,
    scihubingestion numeric,
    starttime numeric,
    stoptime numeric,
    isoceanic boolean,
    oceanintersection geometry,
    CONSTRAINT sns_pkey PRIMARY KEY (sns_messageid),
    CONSTRAINT "sns_grd_id_key" UNIQUE (grd_id),
    CONSTRAINT "sns_grd_uuid_key" UNIQUE (grd_uuid)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.sns
    OWNER to postgres;