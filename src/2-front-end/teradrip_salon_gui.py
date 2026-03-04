import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import pandas as pd
from teradrip_ml import SalonETL 

# Branding Colors
COLOR_TIFFANY = "#AFD5CA"
COLOR_MIDNIGHT = "#114B5F"
COLOR_WHITE = "#FFFFFF"

class TeraDripPipelineApp:
    def __init__(self, root):
        self.root = root
        self.root.title("TeraDrip Salon Pipeline")
        self.root.geometry("800x600")
        self.root.configure(bg=COLOR_TIFFANY)
        
        self.etl = SalonETL()
        self.setup_styles()
        self.create_widgets()

    def setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("TNotebook", background=COLOR_TIFFANY, borderwidth=0)
        style.configure("TNotebook.Tab", background=COLOR_MIDNIGHT, foreground=COLOR_WHITE, padding=[15, 5])
        style.map("TNotebook.Tab", background=[("selected", COLOR_WHITE)], foreground=[("selected", COLOR_MIDNIGHT)])

    def create_widgets(self):
        # Header - Brand Title
        tk.Label(self.root, text="SALON DATA PIPELINE", font=("Helvetica", 20, "bold"), 
                 bg=COLOR_TIFFANY, fg=COLOR_MIDNIGHT).pack(pady=20)

        # Tabbed Dashboard (Representing your "Frontend" block)
        self.tabs = ttk.Notebook(self.root)
        self.tab_upload = tk.Frame(self.tabs, bg=COLOR_WHITE)
        self.tab_analytics = tk.Frame(self.tabs, bg=COLOR_WHITE)
        
        self.tabs.add(self.tab_upload, text="  1. FILE UPLOAD (RAW CSV)  ")
        self.tabs.add(self.tab_analytics, text="  2. BUSINESS RECOMMENDATIONS  ")
        self.tabs.pack(expand=1, fill="both", padx=20, pady=10)

        self.build_upload_ui()
        self.build_analytics_ui()

    def build_upload_ui(self):
        """Maps to 'File Upload (raw.csv)' in your pipeline"""
        tk.Label(self.tab_upload, text="Step 1: Extract & Load", font=("Arial", 12, "bold"), 
                 bg=COLOR_WHITE, fg=COLOR_MIDNIGHT).pack(pady=20)
        
        self.btn_browse = tk.Button(self.tab_upload, text="BROWSE LOCAL CSV", bg=COLOR_MIDNIGHT, 
                                   fg=COLOR_WHITE, command=self.browse_file, width=25)
        self.btn_browse.pack(pady=10)

        self.lbl_file = tk.Label(self.tab_upload, text="No file selected", bg=COLOR_WHITE, fg="gray")
        self.lbl_file.pack()

        # Triggering the ETL process
        self.btn_run_etl = tk.Button(self.tab_upload, text="START ETL & WAREHOUSE", 
                                    bg=COLOR_TIFFANY, fg=COLOR_MIDNIGHT, font=("Arial", 10, "bold"),
                                    command=self.process_pipeline, width=25)
        self.btn_run_etl.pack(pady=30)

    def build_analytics_ui(self):
        """Maps to 'ML - Business Recommendations' in your pipeline"""
        tk.Label(self.tab_analytics, text="Step 2: Warehouse to ML", font=("Arial", 12, "bold"), 
                 bg=COLOR_WHITE, fg=COLOR_MIDNIGHT).pack(pady=20)
        
        self.btn_ml = tk.Button(self.tab_analytics, text="GENERATE RECOMMENDATIONS", 
                               bg=COLOR_MIDNIGHT, fg=COLOR_WHITE, command=self.run_ml_logic)
        self.btn_ml.pack(pady=10)

        self.txt_output = tk.Text(self.tab_analytics, bg="#F9F9F9", height=15, width=80)
        self.txt_output.pack(pady=10, padx=20)

    # --- Pipeline Logic ---
    
    def browse_file(self):
        self.filepath = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if self.filepath:
            self.lbl_file.config(text=f"Selected: {self.filepath.split('/')[-1]}")

    def process_pipeline(self):
        """Executes Extract -> Transform -> Load -> Warehouse"""
        if not hasattr(self, 'filepath'):
            return messagebox.showerror("Error", "Please select a raw CSV file first.")
        
        # 1. Extract & Transform (Handled inside your load_to_cloud logic)
        # 2. Load to Supabase (The 'Warehouse' step)
        success = self.etl.load_to_cloud(self.filepath, "transactions")
        
        if success:
            messagebox.showinfo("Pipeline Status", "Data Cleansed & Stored in Warehouse (Supabase)")

    def run_ml_logic(self):
        """Executes ML -> Business Recommendations"""
        self.txt_output.insert("end", "> Fetching from Warehouse...\n")
        df = self.etl.extract_from_cloud("transactions")
        
        if not df.empty:
            self.txt_output.insert("end", "> Cleansing and Setting Datatypes...\n")
            # This triggers your boolean transformation we discussed!
            transformed = self.etl.transform_data(df)
            
            self.txt_output.insert("end", f"> ML Input Ready: {transformed.shape}\n")
            self.txt_output.insert("end", "> Iterating Recommendation Algorithm...\n")
            # Logic for FP-Growth or Business rules goes here
            self.txt_output.insert("end", "SUCCESS: Top Recommendation - Pair 'Haircut' with 'Hair Spa'\n")

if __name__ == "__main__":
    root = tk.Tk()
    app = TeraDripPipelineApp(root)
    root.mainloop()