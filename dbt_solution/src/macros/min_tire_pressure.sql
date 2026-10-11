{#
  The lowest of the four tire pressures of a reading. Used by
  fct_vehicle_daily and vehicle_latest_status, so the list of tires is
  written only once.
#}
{% macro min_tire_pressure() %}
    least(tire_front_left_kpa, tire_front_right_kpa, tire_rear_left_kpa, tire_rear_right_kpa)
{% endmacro %}
