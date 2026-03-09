

// pt/js/widgets/baseWidget.js
export class BaseWidget {
    constructor(context, widgetId, adapter, selector, manager, config = {}) {

        this.context = context;
        this.manager = manager;
        this.page = this.context.page;
        this.adapter = adapter;
        this.engine = adapter.engine;
        this.widgetId = widgetId;

        this.dynamicController = new AbortController();
        this.abortSignal = this.dynamicController.signal;

        this.isActive = false;
        this.isInitialized = false;
        this.subscriptions = [];

        this.widgetDiv = null;
        this.widgetSelector = selector;
        this.api = null;
    }

    async _init() {
        if (this.isInitialized) return;
        if (!this.widgetSelector) {
            console.error(`Widget container not defined.`);
            return
        }
        this.widgetDiv = document.querySelector(this.widgetSelector);
        if (!this.widgetDiv) {
            console.error(`Widget container ${this.widgetSelector} not found.`);
            return;
        }
        this.onRender()
        await this.onInit();
        if (this.manager?.mounted$?.get('mounted')) {
            await this.afterMount();
        } else {
            console.log('not yet mounted, skipping')
        }

        this.isInitialized = true;
    }

    async activate() {
        if (this.isActive) return;
        if (!this.isInitialized) {
            await this._init()
        }
        this.isActive = true;
        this.onResumeSubscriptions();
        await this.onActivate();
    }

    deactivate() {
        if (!this.isActive) return;
        this.isActive = false;

        this.subscriptions.forEach(sub => sub.unsubscribe());
        this.subscriptions = [];

        this.onDeactivate();
    }

    async cleanup() {
        await this.deactivate();
        await this.onCleanup();
    }

    addSubscription(subscription) {
        if (this.isActive) {
            this.subscriptions.push(subscription);
        } else {
            subscription.unsubscribe();
        }
    }

    getMaxHeight() {
        if (!this?.api) return
        const buffer = 3;
        let rows=buffer; this.api.forEachNodeAfterFilter(() => rows += 1)
        const row_height = this.api?.getGridOption('rowHeight');
        const header = this.api?.getGridOption('headerHeight');
        const pinned = this.api?.getGridOption('pinnedBottomRowData')?.length || 0;
        return (rows*row_height) + (pinned*row_height) + header;
    }

    async onInit() {}
    async afterMount() {}
    onRender() {}
    async onActivate() {}
    onDeactivate() {}
    onResumeSubscriptions() {}
    onCleanup() {}
}
