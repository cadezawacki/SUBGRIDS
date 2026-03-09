
import { ModalManager } from '@/global/js/modalManager.js';
import {NumberFormatter} from '@/utils/NumberFormatter.js';

const WARNING_THRESHOLD = 0.3

export class ConfigurableDropdown {
    constructor(context, targetElement, triggerElement, userConfig = {}, suffix="") {
        this.context = context;
        this.targetElement = typeof targetElement === 'string' ? document.querySelector(targetElement) : targetElement;
        this.triggerElement = typeof triggerElement === 'string' ? document.querySelector(triggerElement) : triggerElement;
        if (!this.targetElement) {
            console.error('ConfigurableDropdown: Target element not found.');
            return;
        }
        this.modalManager = new ModalManager();

        this.instanceId = 'cdd-' + Math.random().toString(36).substring(2, 9);
        this.config = this._mergeConfigs(this._getDefaultConfig(), userConfig);
        this.suffix = suffix;

        this.state = {
            selected: { ...this.config.initialState.selected },
            dropdownOpen: false,
            hasDataForSelection: true, // Will be updated by _updateAndCheckDataAvailability
        };

        // Initialize state for footer controls
        if (this.config.footer) {
            if (this.config.footer.waterfallToggle) {
                this.state[this.config.footer.waterfallToggle.stateKey] = this.config.initialState[this.config.footer.waterfallToggle.stateKey];
            }
            if (this.config.footer.etfAdjustToggle) {
                this.state[this.config.footer.etfAdjustToggle.stateKey] = this.config.initialState[this.config.footer.etfAdjustToggle.stateKey];
            }
            if (this.config.footer.displayModeToggle) {
                this.state[this.config.footer.displayModeToggle.stateKey] = this.config.initialState[this.config.footer.displayModeToggle.stateKey];
            }
        }

        this.dom = {
            wrapper: null,
            trigger: null,
            panel: null,
        };

        this._boundEventHandlers = {
            handleDocumentClick: this._handleDocumentClick.bind(this),
            handleTriggerInteraction: this._handleTriggerInteraction.bind(this),
            handlePanelClick: this._handlePanelClick.bind(this),
        };

        this.raiseElement = this.context.page.raiseWidgetElement;
        this.lowerElement = this.context.page.lowerWidgetElement;
        this.initialized=false;
    }

    _isObject(item) {
        return item && typeof item === 'object' && !Array.isArray(item);
    }

    _mergeConfigs(defaultConfig, userConfig) {
        const merged = { ...defaultConfig };
        for (const key in userConfig) {
            if (userConfig.hasOwnProperty(key)) {
                const userValue = userConfig[key];
                const defaultValue = defaultConfig[key];
                if (this._isObject(userValue) && this._isObject(defaultValue) && !Array.isArray(userValue) && !Array.isArray(defaultValue)) {
                    merged[key] = this._mergeConfigs(defaultValue, userValue);
                } else if (userValue !== undefined) { // User's value (even null or false) overrides default
                    merged[key] = userValue;
                }
            }
        }
        return merged;
    }

    async handleIconClick(iconElement) {
        const infoText = decodeURIComponent(iconElement.dataset.infoText || '');
        const infoHeader = iconElement.dataset.header || '';

        try {
            const result = await this.modalManager.showCustom({
                id: 'refMkt-info-modal' + this.suffix,
                title: `Reference Market: ${infoHeader}`,
                modalClass: 'modal-sm',
                setupContent: (contentArea, dialog, resolve) => {
                    // Create content
                    const contentDiv = document.createElement('div');
                    contentDiv.style.cssText = `
                        padding: 16px 0;
                        line-height: 1.6;
                        white-space: pre-wrap;
                        word-wrap: break-word;
                    `;
                    contentDiv.textContent = infoText || 'No additional information available.';
                    contentArea.appendChild(contentDiv);

                    // Create close button
                    const buttonContainer = document.createElement('div');
                    buttonContainer.className = 'modal-action flex justify-end gap-2 mt-4';

                    const closeButton = document.createElement('button');
                    closeButton.className = 'btn btn-sm btn-primary';
                    closeButton.textContent = 'Close';
                    closeButton.addEventListener('click', () => {
                        dialog.close('close');
                    }, {once: true});

                    buttonContainer.appendChild(closeButton);
                    contentArea.appendChild(buttonContainer);
                }
            });
        } catch (error) {
            console.error('Error showing info modal:', error);
        }
    }

    _getDefaultConfig() {
        return {
            groups: [], // { key, label, options: [{value, label, metricKey?}]}
            metricsData: {}, // { optionMetricKey: { metricName1: value, metricName2: value }}
            initialState: {
                selected: {},
                waterfallEnabled: true,
                etfEnabled: false,
                displayMode: 'coverage',
            },
            currentState: {
                selected: {},
                waterfallEnabled: true,
                tfEnabled: false,
                displayMode: 'coverage',
            },
            placeholder: 'Select...',
            checkDataAvailability: (selected, state, metricsData) => true,
            footer: {
                waterfallToggle: {
                    label: 'Waterfall missing data',
                    stateKey: 'waterfallEnabled',
                },
                etfAdjustToggle: {
                    label: 'Adjust by ETF Premium?',
                    stateKey: 'etfEnabled',
                },
                displayModeToggle: {
                    label: 'Display mode',
                    stateKey: 'displayMode',
                    modes: [
                        { value: 'coverage', label: 'Coverage' },
                        { value: 'usage', label: 'Usage' },
                    ],
                },
                warningMessageText: '⚠️ Missing data for selected market/side combination with no waterfall',
            },
            onChange: (newState) => {},
            onOpen: () => {},
            onClose: () => {},
            classNames: {
                wrapper: 'custom-dropdown-wrapper',
                trigger: 'custom-dropdown-trigger ts-control',
                triggerItem: 'item',
                triggerPlaceholder: 'item-placeholder',
                triggerMissingData: 'missing-data',
                panel: 'custom-dropdown-panel ts-dropdown',
                panelContent: 'ts-dropdown-content',
                panelGroupsContainer: 'custom-dropdown-groups-container',
                optionGroup: 'optgroup',
                optionGroupHeader: 'optgroup-header',
                option: 'option',
                optionActive: 'active',
                optionLabel: 'option-label',
                optionMetric: 'option-metric',
                optionMetricLow: 'option-below-threshold',
                footer: 'dropdown-footer',
                footerSwitches: 'footer-switches',
                switchContainer: 'switch-container',
                switchLabelText: 'switch-label', // Renamed to avoid conflict with <label> element
                switchControl: 'switch',
                switchInput: 'switch-input', // No specific class needed for input, styled via parent
                switchSlider: 'slider',
                toggleGroup: 'toggle-group',
                toggleGroupButton: 'toggle-button',
                toggleGroupButtonActive: 'active',
                warningMessage: 'warning-message',
                warningMessageShow: 'show',
            }
        };
    }

    async init() {
        this.initialized = true;
        this._setupDOM();
        this._attachEventListeners();
        this._setupReactions();
        await this._updateAndCheckDataAvailability();
    }

    async _checkDataAvailability() {
        if (!this.initialized) await this.init();
        if (!this?.config?.adapter) return
        if (!this?.config?.checkDataAvailability) return``
        if (typeof(this?.config?.checkDataAvailability) !== 'function') return
        const dropdown = this;
        const fn = () => dropdown.config.checkDataAvailability(dropdown.state.selected, dropdown.state, dropdown.config.metricsData, dropdown.config.adapter);
        this.config.metricsData = await fn();
    }

    async _updateAndCheckDataAvailability() {
        await this._checkDataAvailability();
        this._updatePanel();
    }

    _setupDOM() {

        this.dom.wrapper = document.createElement('div');
        this.dom.wrapper.style.position = 'relative'; // For panel positioning

        this.dom.panel = document.createElement('div');
        this.dom.panel.className = this.config.classNames.panel;
        this.dom.panel.id = `${this.instanceId}-panel`;
        this.dom.panel.style.position = 'absolute'; // Will be positioned by JS
        this.dom.panel.style.zIndex = '99999999';
        this.dom.panel.style.pointerEvents = 'all';
        this.dom.panel.style.backgroundColor = 'var(--dropdown-bg)';

        this.dom.wrapper.appendChild(this.dom.panel);
        this.targetElement.appendChild(this.dom.wrapper);

        this.dom.trigger = document.getElementById('current-ref-mkt' + this.suffix);

        this._renderPanel();
        this._renderTrigger();
    }

    _renderTrigger() {
        this.dom.trigger.innerHTML = ''; // Clear previous content
        let hasSelection = false;

        this.config.groups.forEach(groupConfig => {
            const selectedValue = this.state.selected[groupConfig.key];
            if (selectedValue) {
                const selectedOption = groupConfig.options.find(opt => opt.value === selectedValue);
                if (selectedOption) {
                    hasSelection = true;
                    const itemEl = document.createElement('div');
                    itemEl.className = this.config.classNames.triggerItem;
                    itemEl.textContent = selectedOption.abbr || selectedOption.label;
                    this.dom.trigger.appendChild(itemEl);
                }
            }
        });

        if (!hasSelection) {
            const placeholderEl = document.createElement('span');
            placeholderEl.className = this.config.classNames.triggerPlaceholder;
            placeholderEl.style.color = "#9ca3af"; // From prototype
            placeholderEl.textContent = this.config.placeholder;
            this.dom.trigger.appendChild(placeholderEl);
        }

        const isMissingData = !this.state.hasDataForSelection &&
            this.config.footer && this.config.footer.waterfallToggle &&
            !this.state[this.config.footer.waterfallToggle.stateKey];

        if (isMissingData) {
            this.dom.trigger.classList.add(this.config.classNames.triggerMissingData);
        } else {
            this.dom.trigger.classList.remove(this.config.classNames.triggerMissingData);
        }
    }

    _updatePanel() {
        if (!this.panelContent || !this.groupsContainer) {
            return this._renderPanel();
        }

        this.config.groups.forEach((groupConfig, groupIndex) => {
            const groupEl = this.groupsContainer.children[groupIndex];
            if (!groupEl) {
                console.warn(`ConfigurableDropdown: Group element at index ${groupIndex} not found during panel update.`);
                return;
            }

            const optionElements = groupEl.querySelectorAll(`.${this.config.classNames.option}`);

            optionElements.forEach(optionEl => {
                const optionValue = optionEl.dataset.value;
                const isSelected = this.state.selected[groupConfig.key] === optionValue;

                optionEl.classList.toggle(this.config.classNames.optionActive, isSelected);
                const radioEl = optionEl.querySelector('input[type="radio"]');
                if (radioEl) {
                    radioEl.checked = isSelected;
                }

                if (groupConfig.showMetrics && this.config.metricsData && this.config.footer?.displayModeToggle?.stateKey && this.state[this.config.footer.displayModeToggle.stateKey]) {
                    const originalOption = groupConfig.options.find(opt => opt.value === optionValue);
                    if (!originalOption) {
                        console.warn(`ConfigurableDropdown: Original option data not found for value ${optionValue} in group ${groupConfig.key}.`);
                        return;
                    }

                    const metricKeyValue = originalOption[groupConfig.metricKeySource || 'value'];
                    const metricDataForOption = this.config.metricsData[metricKeyValue];
                    const currentDisplayMode = this.state[this.config.footer.displayModeToggle.stateKey];
                    let metricSpan = optionEl.querySelector(`.${this.config.classNames.optionMetric}`);

                    if (metricDataForOption && metricDataForOption.hasOwnProperty(currentDisplayMode)) {
                        const metricValue = metricDataForOption[currentDisplayMode];
                        if (!metricSpan) {
                            metricSpan = document.createElement('span');
                            metricSpan.className = this.config.classNames.optionMetric;
                            optionEl.appendChild(metricSpan);
                        }
                        if (metricValue <= WARNING_THRESHOLD) {
                            metricSpan.classList.add(this.config.classNames.optionMetricLow);
                        } else {
                            metricSpan.classList.remove(this.config.classNames.optionMetricLow);
                        }
                        metricSpan.textContent = metricValue != null ? `${NumberFormatter.formatNumber(metricValue*100, {sigFigs:{subOne:1, normal: 0}})}%` : '';
                        metricSpan.style.display = '';
                    } else if (metricSpan) {
                        metricSpan.style.display = 'none';
                    }
                } else {
                    const metricSpan = optionEl.querySelector(`.${this.config.classNames.optionMetric}`);
                    if (metricSpan) {
                        metricSpan.style.display = 'none';
                    }
                }
            });
        });

        const footerEl = this.panelContent.querySelector(`.${this.config.classNames.footer}`);
        if (footerEl && this.config.footer) {
            const wtConfig = this.config.footer.waterfallToggle;
            if (wtConfig && wtConfig.stateKey) {
                const wtCheckbox = footerEl.querySelector(`input[type="checkbox"][data-state-key="${wtConfig.stateKey}"]`);
                if (wtCheckbox) {
                    wtCheckbox.checked = !!this.state[wtConfig.stateKey];
                }
            }

            const etfConfig = this.config.footer.etfAdjustToggle;
            if (etfConfig && etfConfig.stateKey) {
                const etfCheckbox = footerEl.querySelector(`input[type="checkbox"][data-state-key="${etfConfig.stateKey}"]`);
                if (etfCheckbox) {
                    etfCheckbox.checked = !!this.state[etfConfig.stateKey];
                }
            }

            const dmtConfig = this.config.footer.displayModeToggle;
            if (dmtConfig && dmtConfig.stateKey && Array.isArray(dmtConfig.modes)) {
                const currentDisplayMode = this.state[dmtConfig.stateKey];
                const toggleGroup = footerEl.querySelector(`.${this.config.classNames.toggleGroup}[data-state-key="${dmtConfig.stateKey}"]`);
                if (toggleGroup) {
                    const buttons = toggleGroup.querySelectorAll(`.${this.config.classNames.toggleGroup} button`);
                    buttons.forEach(button => {
                        button.classList.toggle(this.config.classNames.toggleGroupButtonActive, button.dataset.modeValue === currentDisplayMode);
                    });
                }
            }
        }

        // if (this.dom.warningMessage && this.config.footer) {
        //     const wtConfig = this.config.footer.waterfallToggle;
        //     const showWarning = wtConfig && // Warning is relevant only if waterfall toggle exists
        //         !this.state.hasDataForSelection &&
        //         !this.state[wtConfig.stateKey];
        //     this.dom.warningMessage.classList.toggle(this.config.classNames.warningMessageShow, !!showWarning);
        // }
    }

    _renderPanel() {

        if (!this.panelContent) {
            this.dom.panel.innerHTML = ''; // Clear previous content
            this.panelContent = document.createElement('div');
            this.panelContent.className = this.config.classNames.panelContent;
        } else {
            this.panelContent.innerHTML = '';
        }

        this.groupsContainer = document.createElement('div');
        this.groupsContainer.className = this.config.classNames.panelGroupsContainer;

        this.config.groups.forEach(groupConfig => {
            const groupEl = document.createElement('div');
            groupEl.className = this.config.classNames.optionGroup;
            groupEl.setAttribute('role', 'group');

            const headerId = `${this.instanceId}-group-${groupConfig.key}-header` + this.suffix;
            const headerEl = document.createElement('div');
            headerEl.className = this.config.classNames.optionGroupHeader;
            headerEl.id = headerId;
            headerEl.innerHTML = `<svg class="radio-header-icon radio-icon" xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24" data-header="${groupConfig.label}" data-info-text="${encodeURIComponent(groupConfig.info)}"><path fill="currentColor" d="M12 17q.425 0 .713-.288T13 16v-4q0-.425-.288-.712T12 11t-.712.288T11 12v4q0 .425.288.713T12 17m0-8q.425 0 .713-.288T13 8t-.288-.712T12 7t-.712.288T11 8t.288.713T12 9m0 13q-2.075 0-3.9-.788t-3.175-2.137T2.788 15.9T2 12t.788-3.9t2.137-3.175T8.1 2.788T12 2t3.9.788t3.175 2.137T21.213 8.1T22 12t-.788 3.9t-2.137 3.175t-3.175 2.138T12 22m0-2q3.35 0 5.675-2.325T20 12t-2.325-5.675T12 4T6.325 6.325T4 12t2.325 5.675T12 20m0-8"/></svg>`
            const headerElText = document.createElement('div');
            headerElText.textContent = groupConfig.label;
            headerEl.appendChild(headerElText);
            groupEl.appendChild(headerEl);

            groupConfig.options.forEach(option => {
                const optionEl = document.createElement('div');
                optionEl.className = this.config.classNames.option;
                optionEl.dataset.group = groupConfig.key;
                optionEl.dataset.value = option.value;
                optionEl.tabIndex = 0; // Make it focusable
                optionEl.setAttribute('role', 'option');

                const isSelected = this.state.selected[groupConfig.key] === option.value;
                if (isSelected) {
                    optionEl.classList.add(this.config.classNames.optionActive);
                }

                const radioId = `${this.instanceId}-radio-${groupConfig.key}-${option.value}` + this.suffix;
                const radioEl = document.createElement('input');
                radioEl.type = 'radio';
                radioEl.name = `${this.instanceId}-${groupConfig.key}`;
                radioEl.checked = isSelected;
                radioEl.tabIndex = -1; // Not directly focusable, parent div is
                radioEl.id = radioId;

                const labelEl = document.createElement('span'); // Changed from label to span for simplicity with parent click
                labelEl.className = this.config.classNames.optionLabel;
                labelEl.textContent = option.label;
                // labelEl.setAttribute('for', radioId); // Not needed if click is on parent

                optionEl.appendChild(radioEl);
                optionEl.appendChild(labelEl);

                if (groupConfig.showMetrics && this.config.metricsData && this.state[this.config.footer?.displayModeToggle?.stateKey]) {
                    const metricKey = option[groupConfig.metricKeySource || 'value'];
                    const metricData = this.config.metricsData[metricKey];
                    const displayMode = this.state[this.config.footer.displayModeToggle.stateKey];
                    if (metricData && metricData.hasOwnProperty(displayMode)) {
                        const metricValue = metricData[displayMode];
                        const metricEl = document.createElement('span');
                        metricEl.className = this.config.classNames.optionMetric;
                        metricEl.textContent = `${metricValue}%`;
                        optionEl.appendChild(metricEl);
                    }
                }
                groupEl.appendChild(optionEl);
            });
            this.groupsContainer.appendChild(groupEl);
        });
        this.panelContent.appendChild(this.groupsContainer);

        if (this.config.footer) {
            this.panelContent.appendChild(this._renderFooter());
        }

        this.dom.panel.appendChild(this.panelContent);
    }

    _renderFooter() {
        const footerEl = document.createElement('div');
        footerEl.className = this.config.classNames.footer;

        const switchesEl = document.createElement('div');
        switchesEl.className = this.config.classNames.footerSwitches;

        // Waterfall Toggle
        const wtConfig = this.config.footer.waterfallToggle;
        if (wtConfig) {
            const container = document.createElement('div');
            container.className = this.config.classNames.switchContainer;

            const labelContainer = document.createElement('div');
            labelContainer.className = 'switch-label-container';

            const labelText = document.createElement('span');
            labelText.className = this.config.classNames.switchLabelText;
            labelText.textContent = wtConfig.label;
            labelContainer.innerHTML = `<svg class="radio-icon radio-footer-icon" data-header="${wtConfig.label}" data-info-text="${encodeURIComponent(wtConfig.info)}" xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"><path fill="currentColor" d="M12 17q.425 0 .713-.288T13 16v-4q0-.425-.288-.712T12 11t-.712.288T11 12v4q0 .425.288.713T12 17m0-8q.425 0 .713-.288T13 8t-.288-.712T12 7t-.712.288T11 8t.288.713T12 9m0 13q-2.075 0-3.9-.788t-3.175-2.137T2.788 15.9T2 12t.788-3.9t2.137-3.175T8.1 2.788T12 2t3.9.788t3.175 2.137T21.213 8.1T22 12t-.788 3.9t-2.137 3.175t-3.175 2.138T12 22m0-2q3.35 0 5.675-2.325T20 12t-2.325-5.675T12 4T6.325 6.325T4 12t2.325 5.675T12 20m0-8"/></svg>`
            labelContainer.appendChild(labelText);
            container.appendChild(labelContainer);

            const switchLabel = document.createElement('label');
            switchLabel.classList.add('label','refmkt-toggle', 'toggle-disabled');
            const switchInput = document.createElement('input');
            switchInput.type = 'checkbox';
            switchInput.checked = this.state[wtConfig.stateKey];
            switchInput.dataset.stateKey = wtConfig.stateKey;
            switchInput.className = this.config.classNames.switchInput;
            switchInput.classList.add('toggle');

            switchLabel.appendChild(switchInput);
            container.appendChild(switchLabel);
            switchesEl.appendChild(container);
        }

        // ETF Toggle
        const etfConfig = this.config.footer.etfAdjustToggle;
        if (etfConfig) {
            const etfContainer = document.createElement('div');
            etfContainer.className = this.config.classNames.switchContainer;

            const etfLabelContainer = document.createElement('div');
            etfLabelContainer.className = 'switch-label-container';

            const etfLabelText = document.createElement('span');
            etfLabelText.className = this.config.classNames.switchLabelText;
            etfLabelText.textContent = etfConfig.label;
            etfLabelContainer.innerHTML = `<svg class="radio-icon radio-footer-icon" data-header="${etfConfig.label}" data-info-text="${encodeURIComponent(etfConfig.info)}" xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"><path fill="currentColor" d="M12 17q.425 0 .713-.288T13 16v-4q0-.425-.288-.712T12 11t-.712.288T11 12v4q0 .425.288.713T12 17m0-8q.425 0 .713-.288T13 8t-.288-.712T12 7t-.712.288T11 8t.288.713T12 9m0 13q-2.075 0-3.9-.788t-3.175-2.137T2.788 15.9T2 12t.788-3.9t2.137-3.175T8.1 2.788T12 2t3.9.788t3.175 2.137T21.213 8.1T22 12t-.788 3.9t-2.137 3.175t-3.175 2.138T12 22m0-2q3.35 0 5.675-2.325T20 12t-2.325-5.675T12 4T6.325 6.325T4 12t2.325 5.675T12 20m0-8"/></svg>`
            etfLabelContainer.appendChild(etfLabelText);
            etfContainer.appendChild(etfLabelContainer);

            const etfSwitchLabel = document.createElement('label');
            etfSwitchLabel.classList.add('label','refmkt-toggle');
            const etfSwitchInput = document.createElement('input');
            etfSwitchInput.type = 'checkbox';
            etfSwitchInput.disabled = true;
            etfSwitchInput.checked = this.state[etfConfig.stateKey];
            etfSwitchInput.dataset.stateKey = etfConfig.stateKey;
            etfSwitchInput.className = this.config.classNames.switchInput;
            etfSwitchInput.classList.add('toggle');

            etfSwitchLabel.appendChild(etfSwitchInput);
            etfContainer.appendChild(etfSwitchLabel);
            switchesEl.appendChild(etfContainer);
        }

        // Display Mode Toggle
        const dmtConfig = this.config.footer.displayModeToggle;
        if (dmtConfig) {
            const container = document.createElement('div');
            container.className = this.config.classNames.switchContainer;

            const dmLabelContainer = document.createElement('div');
            dmLabelContainer.className = 'switch-label-container';

            const labelText = document.createElement('span');
            labelText.className = this.config.classNames.switchLabelText;
            labelText.textContent = dmtConfig.label;
            dmLabelContainer.innerHTML = `<svg class="radio-icon radio-footer-icon" data-header="${dmtConfig.label}" data-info-text="${encodeURIComponent(dmtConfig.info)}" xmlns="http://www.w3.org/2000/svg" width="13" height="13" viewBox="0 0 24 24"><path fill="currentColor" d="M12 17q.425 0 .713-.288T13 16v-4q0-.425-.288-.712T12 11t-.712.288T11 12v4q0 .425.288.713T12 17m0-8q.425 0 .713-.288T13 8t-.288-.712T12 7t-.712.288T11 8t.288.713T12 9m0 13q-2.075 0-3.9-.788t-3.175-2.137T2.788 15.9T2 12t.788-3.9t2.137-3.175T8.1 2.788T12 2t3.9.788t3.175 2.137T21.213 8.1T22 12t-.788 3.9t-2.137 3.175t-3.175 2.138T12 22m0-2q3.35 0 5.675-2.325T20 12t-2.325-5.675T12 4T6.325 6.325T4 12t2.325 5.675T12 20m0-8"/></svg>`
            dmLabelContainer.appendChild(labelText);
            container.appendChild(dmLabelContainer);

            const toggleGroup = document.createElement('div');
            toggleGroup.className = this.config.classNames.toggleGroup;
            toggleGroup.dataset.stateKey = dmtConfig.stateKey;

            dmtConfig.modes.forEach(mode => {
                const button = document.createElement('button');
                button.type = 'button';
                button.textContent = mode.label;
                button.dataset.modeValue = mode.value;
                button.className = this.config.classNames.toggleGroupButton;
                if (this.state[dmtConfig.stateKey] === mode.value) {
                    button.classList.add(this.config.classNames.toggleGroupButtonActive);
                }
                toggleGroup.appendChild(button);
            });
            container.appendChild(toggleGroup);
            switchesEl.appendChild(container);
        }
        footerEl.appendChild(switchesEl);

        // Warning Message
        if (this.config.footer.warningMessageText) {
            const warningEl = document.createElement('div');
            warningEl.className = this.config.classNames.warningMessage;
            warningEl.textContent = this.config.footer.warningMessageText;
            this.dom.warningMessage = warningEl; // Store reference
            footerEl.appendChild(warningEl);
        }
        return footerEl;
    }


    render() {
        this.dom.panel.style.display = 'block';
    }

    _attachEventListeners() {
        this.triggerElement.addEventListener('click', async (e) => {
            return await this._boundEventHandlers.handleTriggerInteraction(e)
        });
        this.dom.panel.addEventListener('click', async (e) => {
            return await this._boundEventHandlers.handlePanelClick(e)
        });
        document.addEventListener('click', this._boundEventHandlers.handleDocumentClick);

        //this.dom.wrapper.addEventListener('focus', () => this.dom.trigger.classList.add('focus'));
        //this.dom.wrapper.addEventListener('blur', () => this.dom.trigger.classList.remove('focus'));
    }

    _removeEventListeners() {
        this.triggerElement.removeEventListener('click', this._boundEventHandlers.handleTriggerInteraction);
        this.dom.panel.removeEventListener('click', this._boundEventHandlers.handlePanelClick);
        document.removeEventListener('click', this._boundEventHandlers.handleDocumentClick);
    }

    _triggerUpdate() {
        this._renderTrigger();
        this.dom.panel ? this._updatePanel() : this._renderPanel();
    }

    _triggerChange() {
        this._triggerUpdate();
        this.config.onChange(this.state);
    }

    _setupReactions() {
        const dropdown = this;
        if (this?.config?.context) {
            this?.config?.context?.page?.activeRefSettingsWaterfall$.onChanges(async (ch)=>{
                await dropdown._updateAndCheckDataAvailability();
            });
        }
    }

    // --- Event Handlers ---
    async _handleTriggerInteraction(event) {
        await this.toggleDropdown();
    }

    _handleDocumentClick(event) {

        if (document.contains(event.target)) {
            const modalElem = event.target.closest('.modal');
            if (!this.dom.wrapper.contains(event.target) && !this.triggerElement.contains(event.target) && this.state.dropdownOpen && !modalElem) {
                this.close();
            }
        }
    }

    async _handlePanelClick(event) {

        const target = event.target;

        // Option selection
        const optionDiv = target.closest(`.${this.config.classNames.option}`);
        if (optionDiv && optionDiv.dataset.group && optionDiv.dataset.value) {
            const groupKey = optionDiv.dataset.group;
            this.state.selected[groupKey] = optionDiv.dataset.value;
            this._triggerChange();
            return;
        }

        const iconElement = target.closest('.radio-icon');
        if (iconElement) {
            await this.handleIconClick(iconElement);
            return;
        }

        // Footer: Waterfall switch
        const wtConfig = this.config.footer?.waterfallToggle;
        if (wtConfig) {
            const switchInput = target.closest(`.${this.config.classNames.switchInput}`);
            if (switchInput && switchInput.dataset.stateKey === wtConfig.stateKey) {
                if (this.state.selected.market !== 'Dynamic') {
                    this.state[wtConfig.stateKey] = switchInput.checked;
                    this._triggerChange();
                    return;
                }
            }
        }

        const etfConfig = this.config.footer?.etfAdjustToggle;
        if (etfConfig) {
            const switchInput = target.closest(`.${this.config.classNames.switchInput}`);
            if (switchInput && switchInput.dataset.stateKey === etfConfig.stateKey) {
                this.state[etfConfig.stateKey] = switchInput.checked;
                this._triggerChange();
                return;
            }
        }

        // Footer: Display mode toggle
        const dmtConfig = this.config.footer?.displayModeToggle;
        if (dmtConfig) {
            const toggleButton = target.closest(`.${this.config.classNames.toggleGroup} button`);
            if (toggleButton && toggleButton.dataset.modeValue) {
                const owningToggleGroup = toggleButton.closest(`.${this.config.classNames.toggleGroup}`);
                if(owningToggleGroup && owningToggleGroup.dataset.stateKey === dmtConfig.stateKey) {
                    this.state[dmtConfig.stateKey] = toggleButton.dataset.modeValue;
                    this._triggerChange();
                    return;
                }
            }
        }
    }

    // --- Public API ---
    async open() {
        if (!this.state.dropdownOpen) {
            this.state.dropdownOpen = true;
            this.render();
            this.raiseElement();
            await this._updateAndCheckDataAvailability();
            this.config?.onOpen(this);
        }
    }

    close() {
        if (this.state.dropdownOpen) {
            this.state.dropdownOpen = false;
            this.dom.panel.style.opacity = 0;
            this.lowerElement();
            this.config?.onClose(this);
            setTimeout(() => {
                this.dom.panel.style.display = 'none';
                this.dom.panel.style.opacity = 1;
            }, 200)
        }
    }

    async toggleDropdown() {
        if (this.state.dropdownOpen) {
            this.close();
        } else {
            await this.open();
        }
    }

    getValue() {
        return { ...this.state.selected };
    }

    setValue(groupKey, value, triggerChange = true, triggerRender = true) {
        if (this.config.groups.find(g => g.key === groupKey)) {
            this.state.selected[groupKey] = value;
            if (triggerChange) {
                this._triggerChange();
            } else if (triggerRender) {
                this.render(); // Just render without firing onChange
            } else {
                this._triggerUpdate();
            }
        } else {
            console.warn(`ConfigurableDropdown: Group key "${groupKey}" not found.`);
        }
    }

    getFullState() {
        return { ...this.state };
    }

    destroy() {
        this._removeEventListeners();
        if (this.dom.wrapper && this.dom.wrapper.parentNode) {
            this.dom.wrapper.parentNode.removeChild(this.dom.wrapper);
        }
        // Nullify DOM references and state to prevent memory leaks
        this.dom = null;
        this.state = null;
        this.config = null;
        this._boundEventHandlers = null;
    }
}

