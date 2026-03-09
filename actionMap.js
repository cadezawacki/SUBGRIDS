
import {BiMap} from "@/utils/bidirectionalMap.js";
export const ACTION_MAP = new BiMap({
    0: 'identify',
    1: 'control',
    2: 'sync',
    3: 'publish',
    4: 'subscribe',
    5: 'unsubscribe',
    6: 'update_filter',
    7: 'error',
    8: 'trash',
    9: 'ack',
    10: 'duplicate',
    11: 'ping',
    12: 'sync_setting',
    13: 'feedback',
    14: 'toast',
    15: 'refresh',
    16: 'push',
    17: 'upload',
    18: 'execute',
    19: 'disconnect',
    20: 'fetch_columns',
    21: 'fetch_schema',
    22: 'micro_publish',
    23: 'micro_subscribe',
    24: 'micro_unsubscribe'
})
