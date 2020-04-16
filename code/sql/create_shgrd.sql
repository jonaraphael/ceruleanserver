-- Table: public.shgrd

-- DROP TABLE public.shgrd;

CREATE TABLE public.shgrd
(
    summary text COLLATE pg_catalog."default",
    beginposition numeric,
    endposition numeric,
    ingestiondate numeric,
    missiondatatakeid numeric,
    orbitnumber numeric,
    lastorbitnumber numeric,
    relativeorbitnumber numeric,
    lastrelativeorbitnumber numeric,
    sensoroperationalmode text COLLATE pg_catalog."default",
    swathidentifier text COLLATE pg_catalog."default",
    orbitdirection text COLLATE pg_catalog."default",
    producttype text COLLATE pg_catalog."default",
    timeliness text COLLATE pg_catalog."default",
    platformname text COLLATE pg_catalog."default",
    platformidentifier text COLLATE pg_catalog."default",
    instrumentname text COLLATE pg_catalog."default",
    instrumentshortname text COLLATE pg_catalog."default",
    filename text COLLATE pg_catalog."default",
    format text COLLATE pg_catalog."default",
    productclass text COLLATE pg_catalog."default",
    polarisationmode text COLLATE pg_catalog."default",
    acquisitiontype text COLLATE pg_catalog."default",
    status text COLLATE pg_catalog."default",
    size text COLLATE pg_catalog."default",
    footprint text COLLATE pg_catalog."default",
    identifier text COLLATE pg_catalog."default",
    uuid text COLLATE pg_catalog."default" NOT NULL,
    ocn_uuid text COLLATE pg_catalog."default",
    CONSTRAINT shgrd_pkey PRIMARY KEY (uuid)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.shgrd
    OWNER to postgres;