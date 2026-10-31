{ config, lib, pkgs, ezNotifyPackage, ... }:

with lib;

let
  cfg = config.services.ez-notify;
in {
  options.services.ez-notify = {
    enable = mkEnableOption "EZ notify service";

    notifyUrl = mkOption {
      type = types.str;
      description = "Bark-compatible server base URL. The script posts JSON to ${NOTIFY_WORKER_URL}/post.";
    };

    notifySignKey = mkOption {
      type = types.str;
      description = "Sign key for the notification server. The script posts JSON to ${NOTIFY_WORKER_URL}/post with the sign key.";
    };

    cloudflareAccountId = mkOption {
      type = types.str;
      description = "Cloudflare account containing the D1 database.";
    };

    cloudflareApiToken = mkOption {
      type = types.str;
      description = "API token needs Account > D1 > Edit permission.";
    };

    cloudflareNotifyD1DatabaseId = mkOption {
      type = types.str;
      description = "D1 database ID, not the database name.";
    };
  };

  config = mkIf cfg.enable {
    systemd.services."ez-notify" = {
      description = "Check Bilibili live rooms and send notify";
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment = {
        NOTIFY_WORKER_URL = cfg.notifyUrl;
        NOTIFY_SIGN_KEY = cfg.notifySignKey;
        CLOUDFLARE_ACCOUNT_ID = cfg.cloudflareAccountId;
        NOTIFY_D1_DATABASE_ID = cfg.cloudflareNotifyD1DatabaseId;
        CLOUDFLARE_API_TOKEN = cfg.cloudflareApiToken;
      };
      serviceConfig = {
        User = "root";
        ExecStart = "${ezNotifyPackage}/bin/ez-notify-backend";
      };
      wantedBy = [ "multi-user.target" ];
    };
  };
}