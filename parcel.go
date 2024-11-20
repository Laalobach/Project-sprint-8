package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// Подготавливаем SQL-запрос для вставки
	query := `
		INSERT INTO parcel (client, status, address, created_at)
		VALUES (?, ?, ?, ?)
	`
	// Выполняем запрос с параметрами
	result, err := s.db.Exec(query, p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		return 0, fmt.Errorf("не удалось добавить посылку: %w", err)
	}

	// Получаем идентификатор последней добавленной записи
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("не удалось получить ID последней записи: %w", err)
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	query := `
		SELECT *
		FROM parcel
		WHERE number = ?
	`

	// заполните объект Parcel данными из таблицы
	p := Parcel{}

	// Выполняем запрос и заполняем объект Parcel
	err := s.db.QueryRow(query, number).Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return p, fmt.Errorf("посылка с номером %d не найдена: %w", number, err)
		}
		return p, fmt.Errorf("ошибка при получении посылки: %w", err)
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// SQL-запрос для получения посылок по клиенту
	query := `
		SELECT *
		FROM parcel
		WHERE client = ?
	`

	// Выполняем запрос
	rows, err := s.db.Query(query, client)
	if err != nil {
		return nil, fmt.Errorf("не удалось получить посылки для клиента %d: %w", client, err)
	}
	defer rows.Close()

	// Результирующий срез
	var res []Parcel

	// Итерируемся по строкам результата
	for rows.Next() {
		var parcel Parcel

		// Считываем данные из строки в объект Parcel
		err := rows.Scan(&parcel.Number, &parcel.Client, &parcel.Status, &parcel.Address, &parcel.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("ошибка при чтении строки: %w", err)
		}
		// Добавляем объект Parcel в срез
		res = append(res, parcel)
	}

	// Проверяем ошибки итерации
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ошибка при обработке результата: %w", err)
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// SQL-запрос для обновления статуса посылки
	query := `
		UPDATE parcel
		SET status = ?
		WHERE number = ?
	`

	// Выполняем запрос
	result, err := s.db.Exec(query, status, number)
	if err != nil {
		return fmt.Errorf("не удалось обновить статус посылки № %d: %w", number, err)
	}

	// Проверяем, что была затронута хотя бы одна строка
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("не удалось проверить затронутые строки: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("посылка с номером %d не найдена", number)
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered

	// Получаем текущий статус посылки
	currentStatus, err := s.GetStatus(number)
	if err != nil {
		return err
	}

	// Проверяем, что статус позволяет изменить адрес
	if currentStatus != ParcelStatusRegistered {
		return fmt.Errorf("нельзя изменить адрес для посылки № %d, её статус: %s", number, currentStatus)
	}

	// SQL-запрос для обновления статуса посылки
	query := `
		UPDATE parcel
		SET address = ?
		WHERE number = ?
	`

	// Выполняем запрос
	result, err := s.db.Exec(query, address, number)
	if err != nil {
		return fmt.Errorf("не удалось обновить адресс посылки № %d: %w", number, err)
	}

	// Проверяем, что была затронута хотя бы одна строка
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("не удалось проверить затронутые строки: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("посылка с номером %d не найдена", number)
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered

	// Шаг 1: Получаем текущий статус посылки
	currentStatus, err := s.GetStatus(number)
	if err != nil {
		return err // Ошибка при получении статуса
	}

	// Шаг 2: Проверяем, что статус позволяет удаление
	if currentStatus != ParcelStatusRegistered {
		return fmt.Errorf("нельзя удалить посылку № %d, её статус: %s", number, currentStatus)
	}

	// Шаг 3: Удаляем запись из таблицы
	deleteQuery := `
		DELETE FROM parcel
		WHERE number = ?
	`
	result, err := s.db.Exec(deleteQuery, number)
	if err != nil {
		return fmt.Errorf("ошибка при удалении посылки № %d: %w", number, err)
	}

	// Шаг 4: Проверяем, что была удалена хотя бы одна строка
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("не удалось проверить затронутые строки: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("посылка с номером %d не найдена", number)
	}

	return nil
}

func (s ParcelStore) GetStatus(number int) (string, error) {
	// SQL-запрос для получения текущего статуса посылки
	query := `
		SELECT status
		FROM parcel
		WHERE number = ?
	`

	// Переменная для хранения статуса
	var status string

	// Выполняем запрос
	err := s.db.QueryRow(query, number).Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("посылка с номером %d не найдена", number)
		}
		return "", fmt.Errorf("ошибка при получении статуса посылки № %d: %w", number, err)
	}

	return status, nil
}
