namespace WinFormsSample
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.chkConfigureAwaitFalse = new System.Windows.Forms.CheckBox();
            this.chkUseOldRequestStyle = new System.Windows.Forms.CheckBox();
            this.numMessages = new System.Windows.Forms.NumericUpDown();
            ((System.ComponentModel.ISupportInitialize)(this.numMessages)).BeginInit();
            this.btnRun = new System.Windows.Forms.Button();
            this.lstScenarios = new System.Windows.Forms.ListBox();
            this.SuspendLayout();
            // 
            // chkConfigureAwait
            // 
            this.chkConfigureAwaitFalse.AutoSize = true;
            this.chkConfigureAwaitFalse.Location = new System.Drawing.Point(12, 139);
            this.chkConfigureAwaitFalse.Name = "chkConfigureAwaitFalse";
            this.chkConfigureAwaitFalse.Size = new System.Drawing.Size(97, 17);
            this.chkConfigureAwaitFalse.TabIndex = 2;
            this.chkConfigureAwaitFalse.Text = "ConfigureAwait(false)";
            this.chkConfigureAwaitFalse.UseVisualStyleBackColor = true;
            // 
            // numMessages
            // 
            this.numMessages.Location = new System.Drawing.Point(12, 113);
            this.numMessages.Maximum = new decimal(new int[] {
                5000000,
                0,
                0,
                0});
            this.numMessages.Minimum = new decimal(new int[] {
                1,
                0,
                0,
                0});
            this.numMessages.Name = "numMessages";
            this.numMessages.Size = new System.Drawing.Size(120, 20);
            this.numMessages.TabIndex = 1;
            this.numMessages.Value = new decimal(new int[] {
                50000,
                0,
                0,
                0});
            this.chkUseOldRequestStyle.AutoSize = true;
            this.chkUseOldRequestStyle.Location = new System.Drawing.Point(12, 162);
            this.chkUseOldRequestStyle.Name = "chkUseOldRequestStyle";
            this.chkUseOldRequestStyle.Size = new System.Drawing.Size(127, 17);
            this.chkUseOldRequestStyle.TabIndex = 4;
            this.chkUseOldRequestStyle.Text = "Use OldRequestStyle";
            this.chkUseOldRequestStyle.UseVisualStyleBackColor = true;
            this.chkUseOldRequestStyle.Checked = false;
            // 
            // btnRun
            // 
            this.btnRun.Location = new System.Drawing.Point(12, 189);
            this.btnRun.Name = "btnRun";
            this.btnRun.Size = new System.Drawing.Size(75, 23);
            this.btnRun.TabIndex = 3;
            this.btnRun.Text = "&Run";
            this.btnRun.UseVisualStyleBackColor = true;
            this.btnRun.Click += new System.EventHandler(this.btnRun_Click);
            // 
            // lstScenarios
            // 
            this.lstScenarios.FormattingEnabled = true;
            this.lstScenarios.Location = new System.Drawing.Point(12, 12);
            this.lstScenarios.Name = "lstScenarios";
            this.lstScenarios.Size = new System.Drawing.Size(120, 95);
            this.lstScenarios.TabIndex = 0;
            // 
            // Form1
            // 
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(264, 348);
            this.Controls.Add(this.chkConfigureAwaitFalse);
            this.Controls.Add(this.chkUseOldRequestStyle);
            this.Controls.Add(this.numMessages);
            this.Controls.Add(this.lstScenarios);
            this.Controls.Add(this.btnRun);
            this.Name = "Form1";
            this.Text = "Sample";
            this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.Form1_FormClosed);
            this.Load += new System.EventHandler(this.Form1_Load);
            ((System.ComponentModel.ISupportInitialize)(this.numMessages)).EndInit();
            this.ResumeLayout(false);
            this.PerformLayout();
        }

        #endregion

        private System.Windows.Forms.CheckBox chkConfigureAwaitFalse;
        private System.Windows.Forms.CheckBox chkUseOldRequestStyle;
        private System.Windows.Forms.NumericUpDown numMessages;
        private System.Windows.Forms.Button btnRun;
        private System.Windows.Forms.ListBox lstScenarios;
    }
}