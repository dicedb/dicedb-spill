#include "valkeymodule.h"
#include <stdio.h>
#include <string.h>

int PreevictionKeyNotification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(type);

    if (strcmp(event, "preeviction") == 0) {
        size_t len;
        const char *keyname = ValkeyModule_StringPtrLen(key, &len);
        ValkeyModule_Log(ctx, "notice", "about to evict key: %.*s", (int)len, keyname);
    }

    return 0;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    
    if (ValkeyModule_Init(ctx, "infcache", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    
    if (ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_PREEVICTION, PreevictionKeyNotification) != VALKEYMODULE_OK) {
        ValkeyModule_Log(ctx, "warning", "Failed to subscribe to eviction events");
        return VALKEYMODULE_ERR;
    }
    
    ValkeyModule_Log(ctx, "notice", "Infcache module loaded successfully");
    
    return VALKEYMODULE_OK;
}