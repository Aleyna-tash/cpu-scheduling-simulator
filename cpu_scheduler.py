"""
CPU ZAMANLAMA SİMÜLATÖRÜ
İşletim Sistemleri Projesi - 6 Farklı Zamanlama Algoritması

Algoritmalar:
1. FCFS (First Come First Served)
2. Preemptive SJF (Shortest Job First)
3. Non-Preemptive SJF
4. Round Robin (Quantum=4)
5. Preemptive Priority Scheduling
6. Non-Preemptive Priority Scheduling
"""

import csv
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import List, Tuple
import os

# Sabitler
CONTEXT_SWITCH_TIME = 0.001  # Bağlam değiştirme süresi
ROUND_ROBIN_QUANTUM = 4      # Round Robin zaman dilimi
PRIORITY_MAP = {'high': 1, 'normal': 2, 'low': 3}  # Küçük sayı = yüksek öncelik

@dataclass
class Process:
    """Süreç bilgilerini tutan sınıf"""
    id: str
    arrival_time: int
    burst_time: int
    priority: int
    remaining_time: int = 0
    start_time: int = -1
    finish_time: int = 0
    waiting_time: int = 0
    turnaround_time: int = 0
    
    def __post_init__(self):
        self.remaining_time = self.burst_time

def load_processes(filename: str) -> List[Process]:
    """CSV dosyasından süreçleri yükler"""
    processes = []
    with open(filename, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            process = Process(
                id=row['Process_ID'],
                arrival_time=int(row['Arrival_Time']),
                burst_time=int(row['CPU_Burst_Time']),
                priority=PRIORITY_MAP[row['Priority'].lower()]
            )
            processes.append(process)
    return processes

def calculate_metrics(processes: List[Process], timeline: List[Tuple], total_time: int):
    """Performans metriklerini hesaplar"""
    # Bekleme ve tamamlanma süreleri
    waiting_times = [p.waiting_time for p in processes]
    turnaround_times = [p.turnaround_time for p in processes]
    
    # Context switch sayısı
    context_switches = len(timeline) - 1
    
    # IDLE zamanı
    idle_time = sum(end - start for start, end, pid in timeline if pid == 'IDLE')
    
    # CPU verimliliği
    total_context_switch_time = context_switches * CONTEXT_SWITCH_TIME
    cpu_efficiency = ((total_time - idle_time) / (total_time + total_context_switch_time)) * 100
    
    # Throughput (T=50, 100, 150, 200)
    throughput = []
    for t in [50, 100, 150, 200]:
        count = sum(1 for p in processes if p.finish_time <= t)
        throughput.append(count)
    
    return {
        'max_waiting': max(waiting_times) if waiting_times else 0,
        'avg_waiting': sum(waiting_times) / len(waiting_times) if waiting_times else 0,
        'max_turnaround': max(turnaround_times) if turnaround_times else 0,
        'avg_turnaround': sum(turnaround_times) / len(turnaround_times) if turnaround_times else 0,
        'throughput': throughput,
        'cpu_efficiency': cpu_efficiency,
        'context_switches': context_switches
    }

def write_results(filename: str, algo_name: str, processes: List[Process], 
                 timeline: List[Tuple], metrics: dict):
    """Sonuçları dosyaya yazar"""
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(f"{'='*80}\n")
        f.write(f"{algo_name} - CPU ZAMANLAMA SONUÇLARI\n")
        f.write(f"{'='*80}\n\n")
        
        # Zaman tablosu
        f.write("ZAMAN TABLOSU:\n")
        f.write("-" * 50 + "\n")
        for start, end, pid in timeline:
            f.write(f"[{start:4d}] -- {pid:6s} -- [{end:4d}]\n")
        
        f.write("\n" + "="*80 + "\n\n")
        
        # Performans metrikleri
        f.write("PERFORMANS METRİKLERİ:\n")
        f.write("-" * 50 + "\n")
        f.write(f"Maksimum Bekleme Süresi: {metrics['max_waiting']:.2f} birim\n")
        f.write(f"Ortalama Bekleme Süresi: {metrics['avg_waiting']:.2f} birim\n")
        f.write(f"Maksimum Tamamlanma Süresi: {metrics['max_turnaround']:.2f} birim\n")
        f.write(f"Ortalama Tamamlanma Süresi: {metrics['avg_turnaround']:.2f} birim\n")
        f.write(f"\nThroughput (İş Tamamlama Sayısı):\n")
        f.write(f"  T=50  : {metrics['throughput'][0]} süreç\n")
        f.write(f"  T=100 : {metrics['throughput'][1]} süreç\n")
        f.write(f"  T=150 : {metrics['throughput'][2]} süreç\n")
        f.write(f"  T=200 : {metrics['throughput'][3]} süreç\n")
        f.write(f"\nCPU Verimliliği: {metrics['cpu_efficiency']:.2f}%\n")
        f.write(f"Toplam Bağlam Değiştirme: {metrics['context_switches']}\n")
        
        f.write("\n" + "="*80 + "\n")

# ============= ALGORİTMA 1: FCFS =============
def fcfs_scheduler(processes: List[Process], case_name: str):
    """First Come First Served algoritması"""
    procs = [Process(p.id, p.arrival_time, p.burst_time, p.priority) 
             for p in processes]
    procs.sort(key=lambda x: x.arrival_time)
    
    current_time = 0
    timeline = []
    
    for p in procs:
        # IDLE zamanı ekle
        if current_time < p.arrival_time:
            timeline.append((current_time, p.arrival_time, 'IDLE'))
            current_time = p.arrival_time
        
        # Süreci çalıştır
        p.start_time = current_time
        p.finish_time = current_time + p.burst_time
        p.turnaround_time = p.finish_time - p.arrival_time
        p.waiting_time = p.turnaround_time - p.burst_time
        
        timeline.append((current_time, p.finish_time, p.id))
        current_time = p.finish_time
    
    metrics = calculate_metrics(procs, timeline, current_time)
    write_results(f'sonuclar_{case_name}_FCFS.txt', 'FCFS', procs, timeline, metrics)
    print(f"✓ FCFS tamamlandı ({case_name})")

# ============= ALGORİTMA 2: Preemptive SJF =============
def sjf_preemptive_scheduler(processes: List[Process], case_name: str):
    """Preemptive Shortest Job First algoritması"""
    procs = [Process(p.id, p.arrival_time, p.burst_time, p.priority) 
             for p in processes]
    
    current_time = 0
    completed = []
    timeline = []
    ready_queue = []
    last_process = None
    
    while len(completed) < len(procs):
        # Gelen süreçleri ekle
        ready_queue.extend([p for p in procs if p.arrival_time == current_time and p not in completed])
        
        if ready_queue:
            # En kısa kalan süreyi seç
            ready_queue.sort(key=lambda x: x.remaining_time)
            current_proc = ready_queue[0]
            
            # Context switch kontrolü
            if last_process != current_proc.id:
                if timeline and timeline[-1][2] != 'IDLE':
                    pass  # Context switch oldu
                last_process = current_proc.id
            
            if current_proc.start_time == -1:
                current_proc.start_time = current_time
            
            # 1 birim çalıştır
            start = current_time
            current_proc.remaining_time -= 1
            current_time += 1
            
            # Timeline'a ekle veya birleştir
            if timeline and timeline[-1][2] == current_proc.id and timeline[-1][1] == start:
                timeline[-1] = (timeline[-1][0], current_time, current_proc.id)
            else:
                timeline.append((start, current_time, current_proc.id))
            
            # Tamamlandı mı?
            if current_proc.remaining_time == 0:
                current_proc.finish_time = current_time
                current_proc.turnaround_time = current_proc.finish_time - current_proc.arrival_time
                current_proc.waiting_time = current_proc.turnaround_time - current_proc.burst_time
                completed.append(current_proc)
                ready_queue.remove(current_proc)
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p not in completed], default=current_time)
            if next_arrival > current_time:
                timeline.append((current_time, next_arrival, 'IDLE'))
                current_time = next_arrival
                last_process = None
    
    metrics = calculate_metrics(completed, timeline, current_time)
    write_results(f'sonuclar_{case_name}_SJF_Preemptive.txt', 
                 'Preemptive SJF', completed, timeline, metrics)
    print(f"✓ Preemptive SJF tamamlandı ({case_name})")

# ============= ALGORİTMA 3: Non-Preemptive SJF =============
def sjf_nonpreemptive_scheduler(processes: List[Process], case_name: str):
    """Non-Preemptive Shortest Job First algoritması"""
    procs = [Process(p.id, p.arrival_time, p.burst_time, p.priority) 
             for p in processes]
    
    current_time = 0
    completed = []
    timeline = []
    ready_queue = []
    
    while len(completed) < len(procs):
        # Gelen süreçleri ekle
        ready_queue.extend([p for p in procs if p.arrival_time <= current_time and p not in completed and p not in ready_queue])
        
        if ready_queue:
            # En kısa burst time'ı seç
            ready_queue.sort(key=lambda x: x.burst_time)
            current_proc = ready_queue.pop(0)
            
            current_proc.start_time = current_time
            current_proc.finish_time = current_time + current_proc.burst_time
            current_proc.turnaround_time = current_proc.finish_time - current_proc.arrival_time
            current_proc.waiting_time = current_proc.turnaround_time - current_proc.burst_time
            
            timeline.append((current_time, current_proc.finish_time, current_proc.id))
            current_time = current_proc.finish_time
            completed.append(current_proc)
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p not in completed], default=current_time)
            if next_arrival > current_time:
                timeline.append((current_time, next_arrival, 'IDLE'))
                current_time = next_arrival
    
    metrics = calculate_metrics(completed, timeline, current_time)
    write_results(f'sonuclar_{case_name}_SJF_NonPreemptive.txt', 
                 'Non-Preemptive SJF', completed, timeline, metrics)
    print(f"✓ Non-Preemptive SJF tamamlandı ({case_name})")

# ============= ALGORİTMA 4: Round Robin =============
def round_robin_scheduler(processes: List[Process], case_name: str):
    """Round Robin algoritması (Quantum=4)"""
    procs = [Process(p.id, p.arrival_time, p.burst_time, p.priority) 
             for p in processes]
    
    current_time = 0
    completed = []
    timeline = []
    ready_queue = deque()
    
    # İlk gelen süreci ekle
    arrived = [p for p in procs if p.arrival_time == 0]
    ready_queue.extend(arrived)
    
    while len(completed) < len(procs) or ready_queue:
        if ready_queue:
            current_proc = ready_queue.popleft()
            
            if current_proc.start_time == -1:
                current_proc.start_time = current_time
            
            # Quantum kadar çalıştır
            time_slice = min(ROUND_ROBIN_QUANTUM, current_proc.remaining_time)
            current_proc.remaining_time -= time_slice
            
            timeline.append((current_time, current_time + time_slice, current_proc.id))
            current_time += time_slice
            
            # Bu süre zarfında gelen süreçleri ekle
            newly_arrived = [p for p in procs if current_time >= p.arrival_time > current_time - time_slice 
                           and p not in completed and p not in ready_queue and p.remaining_time == p.burst_time]
            ready_queue.extend(newly_arrived)
            
            # Tamamlandı mı?
            if current_proc.remaining_time == 0:
                current_proc.finish_time = current_time
                current_proc.turnaround_time = current_proc.finish_time - current_proc.arrival_time
                current_proc.waiting_time = current_proc.turnaround_time - current_proc.burst_time
                completed.append(current_proc)
            else:
                ready_queue.append(current_proc)
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p not in completed], default=current_time)
            if next_arrival > current_time:
                timeline.append((current_time, next_arrival, 'IDLE'))
                current_time = next_arrival
                arrived = [p for p in procs if p.arrival_time == current_time]
                ready_queue.extend(arrived)
    
    metrics = calculate_metrics(completed, timeline, current_time)
    write_results(f'sonuclar_{case_name}_RoundRobin.txt', 
                 'Round Robin (Q=4)', completed, timeline, metrics)
    print(f"✓ Round Robin tamamlandı ({case_name})")

# ============= ALGORİTMA 5: Preemptive Priority =============
def priority_preemptive_scheduler(processes: List[Process], case_name: str):
    """Preemptive Priority Scheduling algoritması"""
    procs = [Process(p.id, p.arrival_time, p.burst_time, p.priority) 
             for p in processes]
    
    current_time = 0
    completed = []
    timeline = []
    ready_queue = []
    last_process = None
    
    while len(completed) < len(procs):
        # Gelen süreçleri ekle
        ready_queue.extend([p for p in procs if p.arrival_time == current_time and p not in completed])
        
        if ready_queue:
            # En yüksek önceliği seç (küçük sayı = yüksek öncelik)
            ready_queue.sort(key=lambda x: (x.priority, x.arrival_time))
            current_proc = ready_queue[0]
            
            if last_process != current_proc.id:
                last_process = current_proc.id
            
            if current_proc.start_time == -1:
                current_proc.start_time = current_time
            
            # 1 birim çalıştır
            start = current_time
            current_proc.remaining_time -= 1
            current_time += 1
            
            # Timeline'a ekle veya birleştir
            if timeline and timeline[-1][2] == current_proc.id and timeline[-1][1] == start:
                timeline[-1] = (timeline[-1][0], current_time, current_proc.id)
            else:
                timeline.append((start, current_time, current_proc.id))
            
            # Tamamlandı mı?
            if current_proc.remaining_time == 0:
                current_proc.finish_time = current_time
                current_proc.turnaround_time = current_proc.finish_time - current_proc.arrival_time
                current_proc.waiting_time = current_proc.turnaround_time - current_proc.burst_time
                completed.append(current_proc)
                ready_queue.remove(current_proc)
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p not in completed], default=current_time)
            if next_arrival > current_time:
                timeline.append((current_time, next_arrival, 'IDLE'))
                current_time = next_arrival
                last_process = None
    
    metrics = calculate_metrics(completed, timeline, current_time)
    write_results(f'sonuclar_{case_name}_Priority_Preemptive.txt', 
                 'Preemptive Priority', completed, timeline, metrics)
    print(f"✓ Preemptive Priority tamamlandı ({case_name})")

# ============= ALGORİTMA 6: Non-Preemptive Priority =============
def priority_nonpreemptive_scheduler(processes: List[Process], case_name: str):
    """Non-Preemptive Priority Scheduling algoritması"""
    procs = [Process(p.id, p.arrival_time, p.burst_time, p.priority) 
             for p in processes]
    
    current_time = 0
    completed = []
    timeline = []
    ready_queue = []
    
    while len(completed) < len(procs):
        # Gelen süreçleri ekle
        ready_queue.extend([p for p in procs if p.arrival_time <= current_time and p not in completed and p not in ready_queue])
        
        if ready_queue:
            # En yüksek önceliği seç
            ready_queue.sort(key=lambda x: (x.priority, x.arrival_time))
            current_proc = ready_queue.pop(0)
            
            current_proc.start_time = current_time
            current_proc.finish_time = current_time + current_proc.burst_time
            current_proc.turnaround_time = current_proc.finish_time - current_proc.arrival_time
            current_proc.waiting_time = current_proc.turnaround_time - current_proc.burst_time
            
            timeline.append((current_time, current_proc.finish_time, current_proc.id))
            current_time = current_proc.finish_time
            completed.append(current_proc)
        else:
            # IDLE
            next_arrival = min([p.arrival_time for p in procs if p not in completed], default=current_time)
            if next_arrival > current_time:
                timeline.append((current_time, next_arrival, 'IDLE'))
                current_time = next_arrival
    
    metrics = calculate_metrics(completed, timeline, current_time)
    write_results(f'sonuclar_{case_name}_Priority_NonPreemptive.txt', 
                 'Non-Preemptive Priority', completed, timeline, metrics)
    print(f"✓ Non-Preemptive Priority tamamlandı ({case_name})")

# ============= ANA PROGRAM =============
def run_all_algorithms(case_file: str, case_name: str):
    """Tüm algoritmaları çalıştırır (tek bir case için)"""
    print(f"\n{'='*60}")
    print(f"  {case_name.upper()} İŞLENİYOR...")
    print(f"{'='*60}")
    
    processes = load_processes(case_file)
    print(f"✓ {len(processes)} süreç yüklendi")
    
    # Her algoritma için thread oluştur
    threads = []
    algorithms = [
        (fcfs_scheduler, "FCFS"),
        (sjf_preemptive_scheduler, "Preemptive SJF"),
        (sjf_nonpreemptive_scheduler, "Non-Preemptive SJF"),
        (round_robin_scheduler, "Round Robin"),
        (priority_preemptive_scheduler, "Preemptive Priority"),
        (priority_nonpreemptive_scheduler, "Non-Preemptive Priority")
    ]
    
    print(f"\n🚀 6 algoritma paralel olarak başlatılıyor...\n")
    
    for algo_func, algo_name in algorithms:
        thread = threading.Thread(target=algo_func, args=(processes, case_name))
        threads.append(thread)
        thread.start()
    
    # Tüm thread'lerin bitmesini bekle
    for thread in threads:
        thread.join()
    
    print(f"\n{'='*60}")
    print(f"  {case_name.upper()} TAMAMLANDI! ✅")
    print(f"{'='*60}\n")

def main():
    """Ana fonksiyon"""
    print("\n" + "="*60)
    print("  CPU ZAMANLAMA SİMÜLATÖRÜ")
    print("  İşletim Sistemleri Projesi")
    print("="*60)
    
    # Case dosyalarını kontrol et
    case_files = [
        ('case1.csv', 'case1'),
        ('case2.csv', 'case2')
    ]
    
    for case_file, case_name in case_files:
        if os.path.exists(case_file):
            run_all_algorithms(case_file, case_name)
        else:
            print(f"⚠️  UYARI: {case_file} bulunamadı, atlanıyor...")
    
    print("\n" + "="*60)
    print("  TÜM SÜREÇLER TAMAMLANDI! 🎉")
    print("  Sonuç dosyaları 'sonuclar_*.txt' olarak kaydedildi")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()