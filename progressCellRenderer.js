import { StateCellEditor } from '@/homepage/js/StateCellRenderer.js';
import { ColorManager } from '@/grids/js/colorManager.js'


export class ProgressCellRenderer {
    constructor() {
        // Default configuration
        this.defaultConfig = {
            type: 'bar', // 'bar' or 'circle'
            showText: true,
            colorThresholds: [
                { min: 1, max: 30, color: 'var(--low-pct)' },    // Red
                { min: 30, max: 70, color: 'var(--med-pct)' },   // Orange
                { min: 70, max: 99, color: 'var(--high-pct)' },   // Green
                { min: 100, max: 100, color: 'var(--done-pct)' }   // Green
            ],
            backgroundColor: 'var(--fallback)',
            textColor: 'var(--text-pct-color)',
            size: 24, // For circle mode
            barHeight: 8
        };

        const b = ColorManager;
    }

    init(params) {
        // Parse configuration from params
        const config = { ...this.defaultConfig, ...(params.config || {}) };

        // Get percentage value (0-100)
        let value = Number(params.value);
        if (typeof value === 'string') value = parseFloat(value);
        if (typeof value !== 'number' || isNaN(value)) value = 0;

        // Handle decimal input (0-1 range) vs percentage (0-100 range)
        if (value <= 1 && value > 0) value *= 100;

        const percentage = Math.max(0, Math.min(100, value));

        // Get color for current percentage
        const fillColor = this.getColorForPercentage(percentage, config.colorThresholds);

        // Create main container
        this.eGui = document.createElement('div');
        this.eGui.className = 'progress-cell-container';

        if (config.type === 'circle') {
            this.createCircleProgress(percentage, fillColor, config);
        } else {
            this.createBarProgress(percentage, fillColor, config);
        }
    }

    createBarProgress(percentage, fillColor, config) {
        this.eGui.style.cssText = 'display:flex;align-items:center;height:100%;padding:0 8px;background:transparent';

        if (config.showText) {
            const textEl = document.createElement('div');
            textEl.className = 'progress-text';
            textEl.textContent = Math.floor(percentage) + '%';
            textEl.style.cssText = `color:${config.textColor};font-size:14px;margin-right:12px;min-width:32px;font-weight:500;white-space:nowrap`;
            this.eGui.appendChild(textEl);
        }

        const barContainer = document.createElement('div');
        barContainer.className = 'progress-bar-container';
        barContainer.style.cssText = `flex:1;height:${config.barHeight}px;background:${config.backgroundColor};border-radius:${config.barHeight/2}px;overflow:hidden;position:relative`;

        if (percentage > 0) {
            const fill = document.createElement('div');
            fill.className = 'progress-bar-fill';
            fill.style.cssText = `width:${percentage}%;height:100%;background:${fillColor};border-radius:${config.barHeight/2}px;transition:width 0.4s ease, background-color 0.4s ease`;
            barContainer.appendChild(fill);
        }

        this.eGui.appendChild(barContainer);
    }

    createCircleProgress(percentage, fillColor, config) {
        this.eGui.style.cssText = 'display:flex;align-items:center;justify-content:center;height:100%;padding:4px;background:transparent';

        const size = config.size;
        const strokeWidth = 3;
        const radius = (size - strokeWidth) / 2;
        const circumference = 2 * Math.PI * radius;
        const offset = circumference - (percentage / 100) * circumference;

        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.style.cssText = `width:${size}px;height:${size}px;transform:rotate(-90deg)`;
        svg.setAttribute('viewBox', `0 0 ${size} ${size}`);

        // Background circle
        const bgCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        bgCircle.setAttribute('cx', size / 2);
        bgCircle.setAttribute('cy', size / 2);
        bgCircle.setAttribute('r', radius);
        bgCircle.setAttribute('stroke', config.backgroundColor);
        bgCircle.setAttribute('stroke-width', strokeWidth);
        bgCircle.setAttribute('fill', 'transparent');

        // Progress circle
        const progressCircle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
        progressCircle.setAttribute('cx', size / 2);
        progressCircle.setAttribute('cy', size / 2);
        progressCircle.setAttribute('r', radius);
        progressCircle.setAttribute('stroke', fillColor);
        progressCircle.setAttribute('stroke-width', strokeWidth);
        progressCircle.setAttribute('fill', `color-mix(in srgb, ${fillColor} calc(100% - var(--fill-opacity)), transparent var(--fill-opacity))`);
        progressCircle.setAttribute('stroke-dasharray', circumference);
        progressCircle.setAttribute('stroke-dashoffset', offset);
        progressCircle.setAttribute('stroke-linecap', 'round');
        progressCircle.style.transition = 'stroke-dashoffset 0.4s ease, stroke 0.4s ease';

        svg.appendChild(bgCircle);
        if (percentage > 0) {
            svg.appendChild(progressCircle);
        }

        if (config.showText) {
            const container = document.createElement('div');
            container.style.cssText = 'position:relative;display:flex;align-items:center;justify-content:center';

            const textEl = document.createElement('div');
            textEl.className = 'progress-text-overlay';
            textEl.textContent = Math.floor(percentage).toFixed(0);
            textEl.style.cssText = `color:color-mix(in srgb, ${fillColor} calc(100% - var(--text-color-mix)), var(--text-color) var(--text-color-mix))`;

            container.appendChild(svg);
            container.appendChild(textEl);
            this.eGui.appendChild(container);
        } else {
            this.eGui.appendChild(svg);
        }
    }

    getColorForPercentage(percentage, thresholds) {
        const p = Math.floor(percentage);
        for (const threshold of thresholds) {
            if (p >= threshold.min && p <= threshold.max) {
                return threshold.color;
            }
            // Handle edge case for 100%
            if (p === 100 && threshold.max === 100 && p >= threshold.min) {
                return threshold.color;
            }
        }
        // Fallback color
        return '#666';
    }

    getGui() {
        return this.eGui;
    }

    // Smooth in-place refresh to avoid flash on pivot grid updates
    refresh(params) {
        const config = { ...this.defaultConfig, ...(params.config || {}) };

        let value = Number(params.value);
        if (typeof value === 'string') value = parseFloat(value);
        if (typeof value !== 'number' || isNaN(value)) value = 0;
        if (value <= 1 && value > 0) value *= 100;
        const percentage = Math.max(0, Math.min(100, value));
        const fillColor = this.getColorForPercentage(percentage, config.colorThresholds);

        if (config.type === 'circle') {
            // Update circle progress in-place
            const progressCircle = this.eGui.querySelector('circle:nth-child(2)');
            const textEl = this.eGui.querySelector('.progress-text-overlay');
            if (progressCircle) {
                const size = config.size || this.defaultConfig.size;
                const strokeWidth = 3;
                const radius = (size - strokeWidth) / 2;
                const circumference = 2 * Math.PI * radius;
                const offset = circumference - (percentage / 100) * circumference;
                progressCircle.setAttribute('stroke-dashoffset', offset);
                progressCircle.setAttribute('stroke', fillColor);
            }
            if (textEl) {
                textEl.textContent = Math.floor(percentage).toFixed(0);
            }
        } else {
            // Update bar progress in-place
            const fill = this.eGui.querySelector('.progress-bar-fill');
            const textEl = this.eGui.querySelector('.progress-text');
            if (fill) {
                fill.style.width = percentage + '%';
                fill.style.background = fillColor;
            } else if (percentage > 0) {
                // Bar didn't exist before (was 0%), create it with transition
                const barContainer = this.eGui.querySelector('.progress-bar-container');
                if (barContainer) {
                    const newFill = document.createElement('div');
                    newFill.className = 'progress-bar-fill';
                    newFill.style.cssText = `width:0%;height:100%;background:${fillColor};border-radius:${config.barHeight/2}px;transition:width 0.4s ease, background-color 0.4s ease`;
                    barContainer.appendChild(newFill);
                    // Force reflow then set target width to trigger animation
                    newFill.offsetWidth;
                    newFill.style.width = percentage + '%';
                }
            }
            if (textEl) {
                textEl.textContent = Math.floor(percentage) + '%';
            }
        }
        return true;
    }
}

// Function-based renderer for maximum performance (bar mode only)
export const progressCellRendererFunction = (params) => {
    const config = {
        colorThresholds: [
            { min: 0, max: 30, color: '#ef4444' },
            { min: 30, max: 70, color: '#f59e0b' },
            { min: 70, max: 100, color: '#10b981' }
        ],
        backgroundColor: '#4a4a4a',
        textColor: '#e0e0e0',
        ...params.config
    };

    let value = params.value;
    if (typeof value === 'string') value = parseFloat(value);
    if (typeof value !== 'number' || isNaN(value)) value = 0;
    if (value <= 1 && value > 0) value *= 100;

    const percentage = Math.max(0, Math.min(100, value));

    // Get color
    let fillColor = '#666';
    for (const threshold of config.colorThresholds) {
        if (percentage >= threshold.min && (percentage < threshold.max || (percentage === 100 && threshold.max === 100))) {
            fillColor = threshold.color;
            break;
        }
    }

    const container = document.createElement('div');
    container.innerHTML = `
        <div style="display:flex;align-items:center;height:100%;padding:0 8px;background:transparent">
            <div style="color:${config.textColor};font-size:14px;margin-right:12px;min-width:32px;font-weight:500">${Math.round(percentage)}%</div>
            <div style="flex:1;height:8px;background:${config.backgroundColor};border-radius:4px;overflow:hidden">
                ${percentage > 0 ? `<div style="width:${percentage}%;height:100%;background:${fillColor};border-radius:4px"></div>` : ''}
            </div>
        </div>
    `;
    return container.firstChild;
};

// Predefined color schemes
export const ColorSchemes = {
    default: [
        { min: 0, max: 30, color: '#ef4444' },    // Red
        { min: 30, max: 70, color: '#f59e0b' },   // Orange
        { min: 70, max: 100, color: '#10b981' }   // Green
    ],
    traffic: [
        { min: 0, max: 33, color: '#dc2626' },    // Red
        { min: 33, max: 66, color: '#eab308' },   // Yellow
        { min: 66, max: 100, color: '#16a34a' }   // Green
    ],
    blue: [
        { min: 0, max: 25, color: '#1e3a8a' },    // Dark blue
        { min: 25, max: 50, color: '#1d4ed8' },   // Blue
        { min: 50, max: 75, color: '#3b82f6' },   // Light blue
        { min: 75, max: 100, color: '#60a5fa' }   // Lighter blue
    ],
    monochrome: [
        { min: 0, max: 100, color: '#6b7280' }    // Single gray
    ]
};

// Usage examples for AG Grid column definitions
// export const ColumnExamples = {
//     // Basic bar progress
//     basicBar: {
//         field: 'completion',
//         headerName: 'Progress',
//         cellRenderer: ProgressCellRenderer,
//         cellRendererParams: {
//             config: {
//                 type: 'bar'
//             }
//         },
//         width: 200
//     },
//
//     // Circle progress with custom colors
//     circleProgress: {
//         field: 'completion',
//         headerName: 'Status',
//         cellRenderer: ProgressCellRenderer,
//         cellRendererParams: {
//             config: {
//                 type: 'circle',
//                 size: 30,
//                 colorThresholds: ColorSchemes.traffic,
//                 showText: true
//             }
//         },
//         width: 120
//     },
//
//     // Custom styled bar
//     customBar: {
//         field: 'completion',
//         headerName: 'Progress',
//         cellRenderer: ProgressCellRenderer,
//         cellRendererParams: {
//             config: {
//                 type: 'bar',
//                 barHeight: 12,
//                 colorThresholds: ColorSchemes.blue,
//                 backgroundColor: '#1f2937',
//                 textColor: '#f9fafb'
//             }
//         },
//         width: 220
//     },
//
//     // High performance function renderer
//     performanceBar: {
//         field: 'completion',
//         headerName: 'Progress',
//         cellRenderer: progressCellRendererFunction,
//         cellRendererParams: {
//             config: {
//                 colorThresholds: ColorSchemes.default
//             }
//         },
//         width: 200
//     }
// };
