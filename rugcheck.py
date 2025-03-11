# rugcheck.py
class RugCheckAPI:
    """
    Implementación dummy de RugCheck para desactivar su funcionalidad.
    Todas las validaciones retornarán valores neutros.
    """
    def __init__(self):
        pass

    def authenticate(self):
        # En lugar de autenticar, se retorna un token dummy
        print("🔕 RugCheck desactivado, se retorna token dummy")
        return "dummy"

    def validate_token_safety(self, token, min_score):
        # Siempre se permite el token
        return True
