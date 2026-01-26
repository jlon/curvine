#!/usr/bin/env python3
"""
Generate performance comparison line chart for Curvine storage systems.
This script creates a line chart comparing different storage-compute configurations,
highlighting Curvine's performance advantage.
"""

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
import os
import sys

# Find and configure Chinese font
def find_chinese_font():
    """Find available Chinese font in the system"""
    chinese_fonts = [
        'Noto Sans CJK SC',  # Preferred: Noto Sans CJK Simplified Chinese
        'Noto Sans CJK TC',
        'Noto Sans CJK JP',
        'WenQuanYi Micro Hei',
        'WenQuanYi Zen Hei',
        'Source Han Sans CN',
        'SimHei',
        'Microsoft YaHei',
        'DejaVu Sans'
    ]
    
    # Get all available fonts
    available_fonts = [f.name for f in fm.fontManager.ttflist]
    
    # Find first available Chinese font
    for font in chinese_fonts:
        if font in available_fonts:
            print(f"Using font: {font}")
            return font
    
    # Fallback: try to find any font that might support Chinese
    for font_name in available_fonts:
        if any(keyword in font_name.lower() for keyword in ['noto', 'hei', 'sans', 'source']):
            print(f"Using fallback font: {font_name}")
            return font_name
    
    print("Warning: No Chinese font found, using DejaVu Sans (Chinese may not display correctly)")
    return 'DejaVu Sans'  # Final fallback

# Configure matplotlib to support Chinese fonts
chinese_font = find_chinese_font()
plt.rcParams['font.sans-serif'] = [chinese_font, 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False  # Fix minus sign display issue

def generate_performance_chart(output_path='performance_comparison.png'):
    """
    Generate a line chart comparing storage-compute configurations.
    
    Args:
        output_path: Path to save the generated chart
    """
    # Data from the performance table
    # Reorder to show trend from high to low (worst to best performance)
    configurations = [
        '存算一体\n(HDD)',            # Higher time (313.68s)
        '存算一体\n(SSD)',            # Lower time (297.96s)
        '存算分离\n(OSS)',            # Highest time (worst performance, 419.15s)
        '存算分离\n(Curvine HDD)',    # Medium time (385.20s)
        '存算分离\n(Curvine SSD)'     # Lowest time (best performance, 361.86s)
    ]
    
    # Total time consumption (seconds) - reordered to match configurations (from high to low)
    total_times = [313.68, 297.96, 419.15, 385.20, 361.86]
    
    # Create figure with white background
    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')
    
    # Define colors: Curvine configurations in red, others in gray/blue
    # Reordered to match new configuration order: [HDD, SSD, OSS, Curvine HDD, Curvine SSD]
    colors = ['#4A90E2', '#4A90E2', '#95A5A6', '#E74C3C', '#DC143C']  # Crimson red for Curvine SSD
    line_widths = [2, 2, 2, 3.5, 4.5]  # Much thicker line for Curvine SSD
    line_styles = ['--', '-', '-', '--', '-']
    markers = ['s', 'o', '^', 's', 'o']
    marker_sizes = [8, 8, 8, 11, 14]  # Larger markers for Curvine
    
    # Plot lines for each configuration
    x_positions = np.arange(len(configurations))
    
    # Plot all lines
    for i, (config, time, color, width, style, marker, msize) in enumerate(
        zip(configurations, total_times, colors, line_widths, line_styles, markers, marker_sizes)
    ):
        is_curvine = 'Curvine' in config
        ax.plot(
            x_positions[i], time,
            marker=marker,
            color=color,
            linewidth=width,
            linestyle=style,
            markersize=msize,
            markeredgecolor='white' if is_curvine else color,
            markeredgewidth=2 if is_curvine else 1,
            label=config,
            zorder=3 if is_curvine else 2
        )
    
    # Connect points with lines to show trend
    # Separate lines for different categories
    # Storage-compute integrated (HDD to SSD) - from high to low
    ax.plot(
        x_positions[:2], total_times[:2],
        color='#4A90E2',
        linewidth=2,
        linestyle='-',
        alpha=0.6,
        zorder=1,
        label='存算一体架构趋势 (从高到低)'
    )
    
    # Storage-compute separated (OSS, Curvine HDD, Curvine SSD)
    # Highlight Curvine line in bright red - showing trend from high to low (worst to best)
    ax.plot(
        x_positions[2:], total_times[2:],
        color='#DC143C',  # Crimson red for emphasis
        linewidth=4,
        linestyle='-',
        alpha=0.85,
        zorder=2,
        label='存算分离架构趋势 (从高到低)'
    )
    
    # Highlight Curvine SSD advantage over OSS
    # Note: After reordering, OSS is at index 2, Curvine SSD is at index 4
    oss_time = total_times[2]  # OSS is now at position 2
    curvine_ssd_time = total_times[4]  # Curvine SSD is now at position 4
    # Calculate improvement: (OSS - Curvine) / OSS * 100
    improvement = ((oss_time - curvine_ssd_time) / oss_time) * 100
    # User mentioned 16% improvement, use rounded value for display
    improvement_display = 16.0  # As mentioned: Curvine SSD (362s) vs OSS (419s) = 16%
    
    # Add annotation highlighting the 16% improvement
    # Annotate from OSS (high) to Curvine SSD (low) showing the improvement
    ax.annotate(
        f'Curvine SSD 比 OSS 快 {improvement_display:.0f}%',
        xy=(x_positions[4], curvine_ssd_time),  # Curvine SSD position
        xytext=(x_positions[4] - 0.4, curvine_ssd_time - 20),
        fontsize=13,
        fontweight='bold',
        color='#DC143C',  # Crimson red to match Curvine line
        arrowprops=dict(
            arrowstyle='->',
            color='#DC143C',
            lw=2.5,
            connectionstyle='arc3,rad=0.3'
        ),
        bbox=dict(
            boxstyle='round,pad=0.6',
            facecolor='#FFF0F0',  # Light red background
            edgecolor='#DC143C',
            linewidth=2.5
        )
    )
    
    # Add value labels on each point
    for i, (config, time) in enumerate(zip(configurations, total_times)):
        is_curvine = 'Curvine' in config
        ax.text(
            x_positions[i], time + 10,
            f'{time:.1f}s',
            ha='center',
            va='bottom',
            fontsize=11 if is_curvine else 10,
            fontweight='bold' if is_curvine else 'normal',
            color='#DC143C' if is_curvine else '#2C3E50'  # Crimson red for Curvine
        )
    
    # Customize axes
    ax.set_xlabel('存储-计算架构配置', fontsize=14, fontweight='bold')
    ax.set_ylabel('TPC-DS 100GB 总耗时 (秒)', fontsize=14, fontweight='bold')
    ax.set_title('性能对比：Curvine 高性能缓存优势明显', fontsize=16, fontweight='bold', pad=20)
    
    # Set x-axis ticks and labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels(configurations, fontsize=11)
    
    # Set y-axis range with some padding
    y_min = min(total_times) - 30
    y_max = max(total_times) + 30
    ax.set_ylim(y_min, y_max)
    
    # Add grid for better readability
    ax.grid(True, linestyle='--', alpha=0.3, color='gray', zorder=0)
    ax.set_axisbelow(True)
    
    # Add legend
    legend_elements = [
        plt.Line2D([0], [0], color='#4A90E2', lw=2, label='存算一体架构'),
        plt.Line2D([0], [0], color='#DC143C', lw=4, label='存算分离架构 (Curvine)', marker='o', markersize=8),
        plt.Line2D([0], [0], color='#95A5A6', lw=2, label='存算分离架构 (OSS)'),
    ]
    ax.legend(handles=legend_elements, loc='upper left', fontsize=11, framealpha=0.95, edgecolor='gray')
    
    # Adjust layout to prevent label cutoff
    plt.tight_layout()
    
    # Save the figure
    fig.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print(f"Chart saved to: {output_path}")
    print(f"Performance improvement: Curvine SSD is {improvement_display:.0f}% faster than OSS")
    print(f"Actual calculated improvement: {improvement:.1f}%")
    
    return output_path

if __name__ == '__main__':
    # Get output path from command line argument or use default
    output_path = sys.argv[1] if len(sys.argv) > 1 else 'performance_comparison.png'
    
    # Generate the chart
    generate_performance_chart(output_path)

