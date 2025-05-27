import pyautogui
import time
import tkinter as tk
from tkinter import ttk # Para um visual um pouco mais moderno
import threading

# --- Variáveis Globais ---
script_rodando = False
thread_automacao = None

# --- Lógica Principal da Automação (do seu script original) ---
def ctrl_v_seta_baixo():
    """Executa CTRL+V e depois pressiona a tecla Seta para Baixo."""
    pyautogui.hotkey('ctrl', 'v')
    time.sleep(0.5)  # Pausa original da sua função
    pyautogui.press('down')

def tarefa_automacao():
    """O loop principal para a automação, controlado pela flag script_rodando."""
    global script_rodando
    print("Tarefa de automação iniciada.")
    try:
        while script_rodando:
            ctrl_v_seta_baixo()  # Executa a ação de CTRL+V e Seta para Baixo

            # Esta pausa corresponde ao time.sleep(0.5) no seu loop interno original
            time.sleep(0.5)

            # Verifica a flag novamente caso tenha sido alterada durante as pausas
            if not script_rodando:
                break
    except Exception as e:
        print(f"Ocorreu um erro na tarefa de automação: {e}")
        # Você poderia atualizar o label de status da GUI aqui, se desejado
    finally:
        print("Tarefa de automação finalizada.")
        # Garante que os elementos da GUI sejam atualizados se a tarefa parar inesperadamente
        # Isso pode ser redundante se parar_script for sempre chamado, mas é bom para robustez
        if janela_principal and botao_iniciar and botao_parar and label_status: # Verifica se os elementos da GUI existem
            try:
                label_status.config(text="Status: Parado")
                botao_iniciar.config(state=tk.NORMAL)
                botao_parar.config(state=tk.DISABLED)
            except tk.TclError: # Lida com o caso em que a janela pode estar fechando
                pass


# --- Funções dos Botões da GUI ---
def iniciar_script():
    """Inicia a tarefa de automação em uma nova thread."""
    global script_rodando, thread_automacao
    if not script_rodando:
        script_rodando = True
        label_status.config(text="Status: Rodando...")
        botao_iniciar.config(state=tk.DISABLED)
        botao_parar.config(state=tk.NORMAL)

        # Inicia a automação em uma nova thread para manter a GUI responsiva
        # daemon=True significa que a thread será finalizada quando o programa principal sair
        thread_automacao = threading.Thread(target=tarefa_automacao, daemon=True)
        thread_automacao.start()
        print("Script iniciado via GUI.")

def parar_script():
    """Para a tarefa de automação."""
    global script_rodando
    if script_rodando:
        script_rodando = False
        # O loop da tarefa_automacao verá que script_rodando é False e sairá.
        # Não é necessário usar join() explicitamente na thread para este tipo de tarefa,
        # pois ela verifica a flag regularmente.

        # Atualiza os elementos da GUI
        # É uma boa prática garantir que estes sejam atualizados, mesmo que a thread
        # também tente atualizá-los ao sair, para fornecer feedback imediato.
        label_status.config(text="Status: Parado.")
        botao_iniciar.config(state=tk.NORMAL)
        botao_parar.config(state=tk.DISABLED)
        print("Script parado via GUI.")

def ao_fechar():
    """Lida com o evento de fechamento da janela da GUI."""
    global script_rodando
    if script_rodando:
        print("Janela fechada, tentando parar o script...")
        parar_script() # Sinaliza para o script parar

    # Opcionalmente, espere a thread terminar se for crítico
    # if thread_automacao and thread_automacao.is_alive():
    #     thread_automacao.join(timeout=1.0) # Espera até 1 segundo

    janela_principal.destroy() # Fecha a janela do Tkinter

# --- Configuração da GUI ---
# Cria a janela principal
janela_principal = tk.Tk()
janela_principal.title("Controle de Automação")
janela_principal.geometry("350x180") # Tamanho ajustado para melhor layout

# Aplica um estilo básico
estilo = ttk.Style()
estilo.configure("TButton", padding=6, relief="flat", font=('Helvetica', 10))
estilo.configure("TLabel", padding=6, font=('Helvetica', 10))

# Frame principal para o conteúdo
frame_principal = ttk.Frame(janela_principal, padding="10 10 10 10")
frame_principal.pack(expand=True, fill=tk.BOTH)

# Label de Status
label_status = ttk.Label(frame_principal, text="Status: Parado", font=('Helvetica', 12, 'bold'))
label_status.pack(pady=15)

# Frame para os botões
frame_botoes = ttk.Frame(frame_principal)
frame_botoes.pack(pady=10)

# Botão Iniciar
botao_iniciar = ttk.Button(frame_botoes, text="Iniciar Script", command=iniciar_script)
botao_iniciar.pack(side=tk.LEFT, padx=10)

# Botão Parar
botao_parar = ttk.Button(frame_botoes, text="Parar Script", command=parar_script, state=tk.DISABLED)
botao_parar.pack(side=tk.LEFT, padx=10)

# Lida com o evento de fechar a janela
janela_principal.protocol("WM_DELETE_WINDOW", ao_fechar)

# --- Executa o loop de eventos do Tkinter ---
if __name__ == "__main__":
    janela_principal.mainloop()