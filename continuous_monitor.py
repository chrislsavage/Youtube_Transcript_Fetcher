"""
Continuous 5-Minute Monitoring Script
Tracks processing progress and reports issues automatically
"""

import time
import json
import os
import subprocess
from datetime import datetime

def get_current_status():
    """Get comprehensive current status."""
    try:
        # Get basic stats
        result = subprocess.run(['python', 'check_status.py'], 
                              capture_output=True, text=True, cwd='.')
        status_output = result.stdout
        
        # Parse key metrics from output
        transcript_count = 0
        processed_count = 0
        failed_count = 0
        completed_playlists = 0
        
        for line in status_output.split('\n'):
            if 'Transcript Files:' in line:
                transcript_count = int(line.split(':')[1].strip())
            elif 'Videos Processed:' in line:
                processed_count = int(line.split(':')[1].strip())
            elif 'Videos Failed:' in line:
                failed_count = int(line.split(':')[1].strip())
            elif 'Completed Playlists:' in line:
                completed_playlists = int(line.split(':')[1].split('/')[0].strip())
        
        # Check if processor is running
        ps_result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        processor_running = 'batch_processor.py' in ps_result.stdout
        
        # Check progress file modification time
        progress_file = 'processing_progress.json'
        if os.path.exists(progress_file):
            mod_time = os.path.getmtime(progress_file)
            last_modified = datetime.fromtimestamp(mod_time).strftime('%H:%M:%S')
            minutes_since_update = (time.time() - mod_time) / 60
        else:
            last_modified = "N/A"
            minutes_since_update = 999
        
        return {
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'transcript_files': transcript_count,
            'videos_processed': processed_count,
            'videos_failed': failed_count,
            'completed_playlists': completed_playlists,
            'processor_running': processor_running,
            'last_progress_update': last_modified,
            'minutes_since_update': minutes_since_update,
            'status_output': status_output
        }
    except Exception as e:
        return {'error': str(e), 'timestamp': datetime.now().strftime('%H:%M:%S')}

def check_for_issues(current_status, previous_status=None):
    """Identify and report issues."""
    issues = []
    
    if 'error' in current_status:
        issues.append(f"❌ STATUS ERROR: {current_status['error']}")
        return issues
    
    # Check if processor is running
    if not current_status['processor_running']:
        issues.append("⚠️ PROCESSOR NOT RUNNING: batch_processor.py not found in processes")
    
    # Check for stalled progress
    if current_status['minutes_since_update'] > 30:
        issues.append(f"⚠️ STALLED: No progress for {current_status['minutes_since_update']:.1f} minutes")
    
    # Check for IP blocking (look at recent log)
    try:
        if os.path.exists('processing_log.txt'):
            with open('processing_log.txt', 'r') as f:
                recent_log = f.read()[-2000:]  # Last 2000 chars
            if 'blocking requests from your IP' in recent_log:
                issues.append("🚫 IP BLOCKED: YouTube is blocking requests")
            if 'Rate limited' in recent_log:
                issues.append("⏳ RATE LIMITED: Exponential backoff in progress")
    except:
        pass
    
    # Compare with previous status for progress detection
    if previous_status and 'error' not in previous_status:
        if current_status['transcript_files'] == previous_status['transcript_files']:
            if current_status['processor_running']:
                issues.append("📊 NO PROGRESS: Files count unchanged but processor running")
    
    return issues

def restart_processor_if_needed(issues):
    """Restart processor if it's not running or severely stalled."""
    should_restart = False
    
    for issue in issues:
        if "PROCESSOR NOT RUNNING" in issue:
            should_restart = True
            break
        elif "STALLED" in issue and "60" in issue:  # Stalled for over 60 minutes
            should_restart = True
            break
    
    if should_restart:
        print("🔄 RESTARTING PROCESSOR...")
        try:
            # Kill any existing process
            subprocess.run(['pkill', '-f', 'batch_processor.py'], stderr=subprocess.DEVNULL)
            time.sleep(3)
            
            # Restart with logging
            subprocess.Popen(['python', 'batch_processor.py', 'resume'], 
                           stdout=open('processing_log.txt', 'w'),
                           stderr=subprocess.STDOUT)
            print("✅ Processor restarted")
            return True
        except Exception as e:
            print(f"❌ Failed to restart: {e}")
            return False
    
    return False

def monitor_continuously():
    """Main monitoring loop."""
    print("🔍 STARTING CONTINUOUS 5-MINUTE MONITORING")
    print("=" * 60)
    
    previous_status = None
    
    while True:
        current_status = get_current_status()
        
        # Print timestamp header
        print(f"\n[{current_status['timestamp']}] 📊 STATUS UPDATE:")
        
        if 'error' in current_status:
            print(f"❌ Error getting status: {current_status['error']}")
        else:
            # Print key metrics
            print(f"   📄 Transcript Files: {current_status['transcript_files']}")
            print(f"   🎥 Videos Processed: {current_status['videos_processed']}")
            print(f"   ❌ Videos Failed: {current_status['videos_failed']}")
            print(f"   ✅ Playlists Completed: {current_status['completed_playlists']}/28")
            print(f"   🔄 Processor Running: {'Yes' if current_status['processor_running'] else 'No'}")
            print(f"   📅 Last Progress: {current_status['last_progress_update']} ({current_status['minutes_since_update']:.1f}m ago)")
            
            # Calculate and show changes
            if previous_status and 'error' not in previous_status:
                file_change = current_status['transcript_files'] - previous_status['transcript_files']
                processed_change = current_status['videos_processed'] - previous_status['videos_processed']
                
                if file_change > 0 or processed_change > 0:
                    print(f"   📈 Changes: +{file_change} files, +{processed_change} processed")
                else:
                    print(f"   📊 No changes detected")
        
        # Check for issues
        issues = check_for_issues(current_status, previous_status)
        
        if issues:
            print(f"   🚨 ISSUES DETECTED:")
            for issue in issues:
                print(f"      {issue}")
            
            # Try to restart if needed
            restarted = restart_processor_if_needed(issues)
            if restarted:
                print(f"   🔄 Attempted automatic restart")
        else:
            print(f"   ✅ No issues detected - processing normally")
        
        previous_status = current_status
        
        # Wait 5 minutes
        print(f"   ⏰ Next update in 5 minutes...")
        time.sleep(300)  # 5 minutes

if __name__ == "__main__":
    try:
        monitor_continuously()
    except KeyboardInterrupt:
        print(f"\n🛑 Monitoring stopped by user")
    except Exception as e:
        print(f"\n💥 Monitoring error: {e}")