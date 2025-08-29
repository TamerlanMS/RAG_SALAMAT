from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):  # type: ignore
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)


class Pharmacy(Base):
    __tablename__ = "pharmacies"
    address: Mapped[str] = mapped_column(unique=True, nullable=False)
    phone: Mapped[str] = mapped_column(unique=True, nullable=True)
    # Связь с таблицей PharmacyProduct
    pharmacy_products = relationship("PharmacyProduct", back_populates="pharmacy")

    def __repr__(self) -> str:
        return f"<Pharmacy(id={self.id}, address={self.address})>"


class Product(Base):
    __tablename__ = "products"
    name: Mapped[str] = mapped_column(unique=True, nullable=False)
    # Связь с таблицей PharmacyProduct
    pharmacy_products = relationship("PharmacyProduct", back_populates="product")

    def __repr__(self) -> str:
        return f"<Product(id={self.id}, name={self.name})>"


class PharmacyProduct(Base):
    __tablename__ = "pharmacy_products"
    product_id: Mapped[int] = mapped_column(ForeignKey("products.id"), nullable=False)
    pharmacy_id: Mapped[int] = mapped_column(
        ForeignKey("pharmacies.id"), nullable=False
    )
    price: Mapped[int] = mapped_column(nullable=False)

    product = relationship("Product", back_populates="pharmacy_products")
    pharmacy = relationship("Pharmacy", back_populates="pharmacy_products")

    __table_args__ = (
        UniqueConstraint("product_id", "pharmacy_id", name="uix_product_pharmacy"),
    )

    def __repr__(self) -> str:
        return (
            f"<PharmacyProduct(pharmacy_address={self.pharmacy.address}, "
            f"product_name={self.product.name}, price={self.price})>"
        )
