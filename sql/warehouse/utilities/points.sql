-- Shared point helpers for PostgreSQL-based clustering logic.
-- This fills the missing dependency referenced by the legacy feature scripts.

DROP FUNCTION IF EXISTS public.point_eq(point, point);
CREATE FUNCTION public.point_eq(left_point point, right_point point)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT left_point ~= right_point;
$$;

DROP OPERATOR IF EXISTS = (point, point);
CREATE OPERATOR = (
  LEFTARG = point,
  RIGHTARG = point,
  PROCEDURE = public.point_eq,
  COMMUTATOR = '='
);

DROP FUNCTION IF EXISTS public.point_accum(double precision[], point);
CREATE FUNCTION public.point_accum(state double precision[], value point)
RETURNS double precision[]
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN value IS NULL THEN COALESCE(state, ARRAY[0::double precision, 0::double precision, 0::double precision])
    ELSE ARRAY[
      COALESCE(state[1], 0::double precision) + value[0]::double precision,
      COALESCE(state[2], 0::double precision) + value[1]::double precision,
      COALESCE(state[3], 0::double precision) + 1::double precision
    ]
  END;
$$;

DROP FUNCTION IF EXISTS public.point_avg_final(double precision[]);
CREATE FUNCTION public.point_avg_final(state double precision[])
RETURNS point
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN state IS NULL OR COALESCE(state[3], 0::double precision) = 0::double precision THEN NULL::point
    ELSE point(state[1] / state[3], state[2] / state[3])
  END;
$$;

DROP AGGREGATE IF EXISTS avg(point);
CREATE AGGREGATE avg(point) (
  SFUNC = public.point_accum,
  STYPE = double precision[],
  FINALFUNC = public.point_avg_final,
  INITCOND = '{0,0,0}'
);

