from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="ENTROPY",
    settings_files=[".entropy/settings.toml", "settings.toml", ".secrets.toml"],
)

# `envvar_prefix` = export envvars with `export DYNACONF_FOO=bar`.
# `settings_files` = Load this files in the order.
